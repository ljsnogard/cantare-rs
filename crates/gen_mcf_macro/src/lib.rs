use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, GenericArgument, GenericParam, Ident, ItemFn, Lifetime, PatType,
    Path, PathArguments, PredicateLifetime, PredicateType, Token, TraitBound,
    Type, TypeArray, TypeParamBound, WhereClause, WherePredicate,
    parse_macro_input, parse_quote, punctuated::Punctuated,
};

#[proc_macro_attribute]
pub fn gen_may_cancel_future(
    attr: TokenStream,
    item: TokenStream,
) -> TokenStream {
    let prefix_args = parse_macro_input!(attr with Punctuated::<Path, Token![,]>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);

    // 要生成的各个 struct 的名称前缀，在调用宏时在代码中指定
    let prefix_ident = if prefix_args.len() == 1 {
        prefix_args
            .first()
            .unwrap()
            .get_ident()
            .cloned()
            .expect("Expected identifier as path")
    } else {
        panic!("Expected exactly one identifier as prefix");
    };

    // 检查输入的函数是否有 async 修饰
    if input_fn.sig.asyncness.is_none() {
        panic!(
            "`#[gen_may_cancel_future]` can only be applied to async functions"
        );
    }

    // 提取函数签名的各个部分
    // 函数名称
    let fn_ident = &input_fn.sig.ident;

    // 函数的泛型参数（包括生命周期）
    let fn_generics = &input_fn.sig.generics;

    // 必备的 where 子句至少有一行，例如 C: TrCancellationToken
    let Option::Some(where_clause) = &input_fn.sig.generics.where_clause else {
        panic!("Function must have where clause for generics");
    };

    let sig_inputs = &input_fn.sig.inputs;
    let sig_output = &input_fn.sig.output;

    // 提取输入函数的泛型参数，包括生命周期
    let (generics_all, generics_no_cancel, lifetimes_all) = {
        let mut generics_all = vec![];
        let mut generics_no_cancel = vec![];
        let mut lifetimes_all = vec![];
        for (i, param) in fn_generics.params.iter().enumerate() {
            if let GenericParam::Type(ty) = param {
                generics_all.push(ty.ident.clone());

                if i < fn_generics.params.len() - 1 {
                    generics_no_cancel.push(ty.ident.clone());
                }
                // Currently we don't have reliable check the type bound for the
                // last parameter `C: TrCancellationToken`. We simply assume it is
                // the last one and always correct.
            }
            if let GenericParam::Lifetime(lt) = param {
                lifetimes_all.push(lt.lifetime.clone());
            }
        }
        if generics_all.is_empty() {
            panic!("Function must have at least one generic parameter");
        }
        if lifetimes_all.is_empty() {
            panic!("Function must have at least one named lifetime");
        }
        (generics_all, generics_no_cancel, lifetimes_all)
    };

    // 根据约定，最后一个生命周期是最短的，同时也是对 cancel_token 的引用的存活
    let last_lt = lifetimes_all.last().unwrap().clone();

    // 根据约定，最后一个泛型参数是用于约束 cancel_token 为 TrCancellation
    let cancel_type_param = generics_all.last().unwrap().clone();

    // 将 where 子句中涉及生命周期的、涉及 cancel_token 类型的全部删除，由此得出
    // async_struct 的泛型约束
    let where_clause_no_cancel_no_lt = {
        let punctuated = where_clause
            .predicates
            .iter()
            .filter(|pred| {
                !predicate_contains_type_param(pred, &cancel_type_param)
                    && !predicate_contains_lifetime(pred, &last_lt)
            })
            .cloned()
            .collect::<Punctuated<_, Token![,]>>();
        if !punctuated.is_empty() {
            WhereClause {
                where_token: where_clause.where_token,
                predicates: punctuated,
            }
        } else {
            // A dummy where clause
            parse_quote! {
                where 'static: 'static
            }
        }
    };
    // 将 where 子句中涉及生命周期的全部删除，得出 Future 和 FutureState 的泛型约束
    let where_clause_no_lt = {
        let punctuated = where_clause
            .predicates
            .iter()
            .filter(|pred| !predicate_contains_lifetime(pred, &last_lt))
            .cloned()
            .collect::<Punctuated<_, Token![,]>>();
        WhereClause {
            where_token: where_clause.where_token,
            predicates: punctuated,
        }
    };

    // 定义 async struct 所需字段、类型
    let mut fields = vec![];
    let mut types = vec![];
    let mut args = vec![];

    let mut cancel_type = None;
    // let mut cancel_pat = None;

    for (i, input_arg) in sig_inputs.iter().enumerate() {
        match input_arg {
            FnArg::Typed(PatType { pat, ty, .. }) => {
                let is_last = i == sig_inputs.len() - 1;

                if is_last {
                    // Expect: Pin<&'f mut C>
                    // if let Type::Path(TypePath { qself: None, path }) = &**ty {
                    //     let Option::Some(last_seg) = path.segments.last() else {
                    //         panic!("Last argument check: must be Pin<&mut C>");
                    //     };
                    //     if last_seg.ident != "Pin" {
                    //         panic!("Last argument check: Pin");
                    //     }
                    //     let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = &last_seg.arguments else {
                    //         panic!("Last argument check: AngleBracketed(AngleBracketedGenericArguments) ")
                    //     };
                    //     if args.len() != 1 {
                    //         panic!("Last argument check: Pin type generic args count")
                    //     }
                    //     let GenericArgument::Type(Type::Reference(cancel_type_ref)) = &args[0] else {
                    //         panic!("Last argument check: Pin type generic args content")
                    //     };
                    //     if cancel_type_ref.mutability.is_none() {
                    //         panic!("Last argument check: mut not found");
                    //     }
                    //     let Option::Some(lt_arg) = cancel_type_ref.lifetime.as_ref() else {
                    //         panic!("Last argument check: lifetime missing");
                    //     };
                    //     if lt_arg.ident != last_lt.ident {
                    //         panic!("Last argument check: lifetime of cancellation token must be the last one");
                    //     }
                    //     let Type::Path(generic_cancel_type_path) = cancel_type_ref.elem.as_ref() else {
                    //         panic!("Last argument check: cancel token type must be simple type token");
                    //     };
                    //     if generic_cancel_type_path.path.segments.len() != 1 {
                    //         panic!("Last argument check: cancel token type should be generic type");
                    //     }
                    //     let cancel_tok_type_ident = &generic_cancel_type_path.path.segments[0].ident;
                    //     if !generics_all.contains(cancel_tok_type_ident) {
                    //         panic!("Last argument check: cancel token type mismatch");
                    //     }
                    // }

                    // Expect: &'f mut C
                    let syn::Type::Reference(cancel_type_ref) = &**ty else {
                        panic!(
                            "Last argument check: not reference type, must be like `&'f mut C`"
                        )
                    };
                    // 3. 检查是否带有 `mut` 关键字
                    if cancel_type_ref.mutability.is_none() {
                        panic!(
                            "Last argument check: not mut ref type, must be like `&'f mut C`"
                        )
                    };
                    let Option::Some(lt_arg) = &cancel_type_ref.lifetime else {
                        panic!(
                            "Last argument check: lifetime not fount, must declare a lifetime like `&'f mut C`"
                        )
                    };
                    if lt_arg.ident != last_lt.ident {
                        panic!(
                            "Last argument check: lifetime of cancellation token must be the last one"
                        );
                    }
                    let Type::Path(generic_cancel_type_path) =
                        cancel_type_ref.elem.as_ref()
                    else {
                        panic!(
                            "Last argument check: cancel token type must be simple type token"
                        );
                    };
                    if generic_cancel_type_path.path.segments.len() != 1 {
                        panic!(
                            "Last argument check: cancel token type should be generic type param"
                        );
                    }
                    let cancel_tok_type_ident =
                        &generic_cancel_type_path.path.segments[0].ident;
                    if !generics_all.contains(cancel_tok_type_ident) {
                        panic!(
                            "Last argument check: cancel token type mismatch"
                        );
                    }
                    cancel_type = Option::Some(ty.clone());
                    // cancel_pat = Some(pat.clone());
                } else {
                    let orig_ty = ty.clone();
                    // 转换外层引用生命周期
                    let transformed_ty =
                        transform_type_outer_lifetime(&orig_ty, &last_lt);
                    fields.push(transformed_ty.clone());
                    types.push(transformed_ty);
                    args.push(pat.clone());
                }
            }
            _ => panic!("Unsupported argument format"),
        }
    }

    let field_indices: Vec<syn::Index> =
        (0..args.len()).map(syn::Index::from).collect();

    let async_struct = format_ident!("{}Async", prefix_ident);
    let future_struct = format_ident!("{}Future", prefix_ident);
    let state_struct = format_ident!("{}FutureState", prefix_ident);

    // Final generic types
    // let gen_params = quote! { #(#generics_all),* };
    // let gen_params_with_lt = quote! { #lt, #(#generics_all),* };
    let output_ty = match sig_output {
        syn::ReturnType::Type(_, ty) => ty,
        _ => panic!("Expected function to return a value"),
    };

    // async_struct 只需要包含字段中实际出现的生命周期；future 需要包含全部
    // 生命周期，因为 cancel token 字段会使用最后一个生命周期 `last_lt`。
    let async_lifetimes = collect_used_lifetimes(&types, &lifetimes_all);
    let generic_params_async_no_cancel =
        build_generic_params(&async_lifetimes, &generics_no_cancel);
    let generic_params_future_no_cancel =
        build_generic_params(&async_lifetimes, &generics_no_cancel);
    let generic_params_future_all =
        build_generic_params(&async_lifetimes, &generics_all);

    let cancel_type_lt_replaced =
        transform_type_outer_lifetime(cancel_type.as_ref().unwrap(), &last_lt);

    // 返回类型仍按原有逻辑改写：在只携带部分生命周期的 async/future 泛型中，
    // 返回类型里出现的用户生命周期统一指向最后一个生命周期。若以后需要完整保留
    // 多个生命周期，可在此处改为直接 clone output_ty。
    let output_ty_transformed =
        transform_output_lifetimes(output_ty, &lifetimes_all, &last_lt);

    let tuple_idents: Vec<Ident> = field_indices
        .iter()
        .map(|idx| format_ident!("p{}", idx.index))
        .collect();
    // 只生成 (p0, p1, ...) 部分
    let tuple_pattern = quote! { ( #(#tuple_idents),* ) };
    let async_struct_destruct = quote! { #async_struct::<#generic_params_async_no_cancel>#tuple_pattern };

    // `IntoFuture::IntoFuture` 的类型：`Future<'c, A, B, ..., NonCancellableToken>`。
    let into_future_ty = quote! {
        #future_struct<#generic_params_future_no_cancel, abs_cancel::NonCancellableToken>
    };

    let expanded = quote! {
        // panic!("input_fn 是: {:#?}", input_fn);
        #input_fn
        // panic!("lt_no_last 结构是: {:#?}\ngenerics_no_cancel 结构是: {:#?}", lt_no_last, generics_no_cancel);
        pub struct #async_struct<#generic_params_async_no_cancel>(#(#fields),*)
        #where_clause_no_cancel_no_lt;

        pub struct #future_struct<#generic_params_future_all>
        #where_clause_no_lt
        {
            params_: ::core::mem::MaybeUninit<#async_struct<#generic_params_async_no_cancel>>,
            cancel_: #cancel_type_lt_replaced,
            future_: Option<<#state_struct<#generic_params_future_all> as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture>,
        }

        // Declair #state_struct
        struct #state_struct<#generic_params_future_all>(::core::pin::Pin<&#last_lt mut #future_struct<#generic_params_future_all>>)
        #where_clause_no_lt;

        // Implement `IntoFuture` for #async_struct
        impl<#generic_params_future_no_cancel> ::core::future::IntoFuture for #async_struct<#generic_params_async_no_cancel>
        #where_clause_no_cancel_no_lt
        {
            type IntoFuture = #into_future_ty;
            type Output = #output_ty_transformed;

            fn into_future(self) -> Self::IntoFuture {
                #future_struct {
                    params_: ::core::mem::MaybeUninit::new(self),
                    cancel_: abs_cancel::NonCancellableToken::shared_mut(),
                    future_: Option::None,
                }
            }
        }

        // Implement `TrMayCancel<'a>` for #async_struct
        impl<#generic_params_future_no_cancel> abs_cancel::TrMayCancel<#last_lt> for #async_struct<#generic_params_async_no_cancel>
        #where_clause_no_cancel_no_lt
        {
            type MayCancelFuture<'cancel_, C> =
                #future_struct<#generic_params_future_no_cancel, C>
            where
                Self: 'cancel_,
                C: abs_cancel::TrCancellationToken + Clone,
                C: #last_lt,
                C: 'cancel_,
                'cancel_: #last_lt;
            type MayCancelOutput = #output_ty_transformed;

            fn may_cancel_with<'cancel_, C>(
                self,
                cancel: &'cancel_ mut C,
            ) -> Self::MayCancelFuture<'cancel_, C>
            where
                Self: 'cancel_,
                // 与 `abs_cancel::TrMayCancel::may_cancel_with` 的 `'f: 'a` 约束对应：
                // cancel token 的借用必须存活不短于数据生命周期 `last_lt`，
                // 否则无法以 `&#last_lt mut C` 的形式保存到生成的 future 里。
                'cancel_: #last_lt,
                C: abs_cancel::TrCancellationToken + Clone,
            {
                #future_struct {
                    params_: ::core::mem::MaybeUninit::new(self),
                    cancel_: cancel,
                    future_: Option::None,
                }
            }
        }

        // Implement `Future` for #future_struct
        impl<#generic_params_future_all> ::core::future::Future for #future_struct<#generic_params_future_all>
        #where_clause_no_lt
        {
            type Output = #output_ty_transformed;

            fn poll(
                self: ::core::pin::Pin<&mut Self>,
                cx: &mut ::core::task::Context<'_>,
            ) -> ::core::task::Poll<Self::Output> {
                let mut this = unsafe {
                    let p = self.get_unchecked_mut();
                    ::core::ptr::NonNull::new_unchecked(p)
                };
                loop {
                    let mut fut_field_ptr = unsafe {
                        let ptr = &mut this.as_mut().future_;
                        ::core::ptr::NonNull::new_unchecked(ptr)
                    };
                    let opt_fut = unsafe { fut_field_ptr.as_mut() };
                    if let Option::Some(fut) = opt_fut {
                        let fut_pin = unsafe { ::core::pin::Pin::new_unchecked(fut) };
                        break fut_pin.poll(cx)
                    } else {
                        let state = #state_struct(unsafe {
                            ::core::pin::Pin::new_unchecked(this.as_mut())
                        });
                        let fut = AsyncFnOnce::async_call_once(state, ());
                        let fut_field_mut = unsafe { fut_field_ptr.as_mut() };
                        *fut_field_mut = Option::Some(fut);
                    }
                }
            }
        }

        impl<#generic_params_future_all> ::core::ops::AsyncFnOnce<()> for #state_struct<#generic_params_future_all>
        #where_clause_no_lt
        {
            type Output = #output_ty_transformed;
            type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;

            extern "rust-call" fn async_call_once(self, _: ()) -> Self::CallOnceFuture {
                let f = unsafe { self.0.get_unchecked_mut() };
                let #async_struct_destruct = unsafe { f.params_.assume_init_read() };
                self::#fn_ident(#(#tuple_idents),*, f.cancel_)
            }
        }
    };

    TokenStream::from(expanded)
}

/// 判断一个类型中是否包含指定的生命周期
fn ty_contains_lifetime(ty: &Type, target_lt: &Lifetime) -> bool {
    match ty {
        Type::Reference(ty_ref) => {
            if let Some(lt) = &ty_ref.lifetime
                && lt.ident == target_lt.ident
            {
                return true;
            }
            ty_contains_lifetime(&ty_ref.elem, target_lt)
        }
        Type::Path(type_path) => {
            for seg in &type_path.path.segments {
                if let PathArguments::AngleBracketed(args) = &seg.arguments {
                    for arg in &args.args {
                        match arg {
                            GenericArgument::Lifetime(lt)
                                if lt.ident == target_lt.ident =>
                            {
                                return true;
                            }
                            GenericArgument::Type(ty)
                                if ty_contains_lifetime(ty, target_lt) =>
                            {
                                return true;
                            }
                            _ => {}
                        }
                    }
                }
            }
            false
        }
        // 其他类型（如元组、数组等）可以类似递归，但为简洁略写
        _ => false,
    }
}

/// 从一组类型中收集实际使用到的生命周期，保持原声明顺序。
fn collect_used_lifetimes(
    types: &[Type],
    all_lifetimes: &[Lifetime],
) -> Vec<Lifetime> {
    all_lifetimes
        .iter()
        .filter(|lt| types.iter().any(|ty| ty_contains_lifetime(ty, lt)))
        .cloned()
        .collect()
}

/// 判断一个 WherePredicate 是否包含指定的生命周期
fn predicate_contains_lifetime(
    pred: &WherePredicate,
    target_lt: &Lifetime,
) -> bool {
    match pred {
        WherePredicate::Lifetime(PredicateLifetime {
            lifetime,
            bounds,
            ..
        }) => {
            if lifetime.ident == target_lt.ident {
                return true;
            }
            for bound in bounds {
                if bound.ident == target_lt.ident {
                    return true;
                }
            }
            false
        }
        WherePredicate::Type(PredicateType {
            bounded_ty, bounds, ..
        }) => {
            if ty_contains_lifetime(bounded_ty, target_lt) {
                return true;
            }
            for bound in bounds {
                match bound {
                    TypeParamBound::Lifetime(lt)
                        if lt.ident == target_lt.ident =>
                    {
                        return true;
                    }
                    TypeParamBound::Trait(TraitBound { path, .. }) => {
                        // 检查 trait 路径中是否包含目标生命周期
                        for seg in &path.segments {
                            if let PathArguments::AngleBracketed(args) =
                                &seg.arguments
                            {
                                for arg in &args.args {
                                    if let GenericArgument::Lifetime(lt) = arg
                                        && lt.ident == target_lt.ident
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            false
        }
        _ => false,
    }
}

/// 检查类型中是否出现指定的类型参数名
fn ty_contains_type_param(ty: &Type, target_ident: &Ident) -> bool {
    match ty {
        Type::Path(type_path) => {
            // 检查路径的最后一个段是否是目标类型参数
            if let Some(seg) = type_path.path.segments.last()
                && seg.ident == *target_ident
            {
                return true;
            }
            // 递归检查路径中的泛型参数
            for seg in &type_path.path.segments {
                if let PathArguments::AngleBracketed(args) = &seg.arguments {
                    for arg in &args.args {
                        match arg {
                            GenericArgument::Type(ty)
                                if ty_contains_type_param(ty, target_ident) => {
                                    return true;
                                }
                            GenericArgument::Lifetime(lt)
                                // 生命周期一般不会直接匹配类型参数名，但保留逻辑
                                if lt.ident == *target_ident => {
                                    return true;
                                }
                            _ => {}
                        }
                    }
                }
            }
            false
        }
        Type::Reference(ty_ref) => {
            ty_contains_type_param(&ty_ref.elem, target_ident)
        }
        Type::Slice(ty_slice) => {
            ty_contains_type_param(&ty_slice.elem, target_ident)
        }
        Type::Tuple(tuple) => {
            for elem in &tuple.elems {
                if ty_contains_type_param(elem, target_ident) {
                    return true;
                }
            }
            false
        }
        // 可根据需要补充其他 Type 变体
        _ => false,
    }
}

/// 检查谓词中是否包含指定的类型参数
fn predicate_contains_type_param(
    pred: &WherePredicate,
    target_ident: &Ident,
) -> bool {
    match pred {
        WherePredicate::Type(PredicateType {
            bounded_ty, bounds, ..
        }) => {
            if ty_contains_type_param(bounded_ty, target_ident) {
                return true;
            }
            for bound in bounds {
                match bound {
                    TypeParamBound::Trait(TraitBound { path, .. }) => {
                        // 检查 trait 路径中是否出现目标类型参数
                        for seg in &path.segments {
                            if seg.ident == *target_ident {
                                return true;
                            }
                            if let PathArguments::AngleBracketed(args) =
                                &seg.arguments
                            {
                                for arg in &args.args {
                                    if let GenericArgument::Type(ty) = arg
                                        && ty_contains_type_param(
                                            ty,
                                            target_ident,
                                        )
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    TypeParamBound::Lifetime(_) => {}
                    _ => {}
                }
            }
            false
        }
        WherePredicate::Lifetime(PredicateLifetime {
            lifetime,
            bounds,
            ..
        }) => {
            if lifetime.ident == *target_ident {
                return true;
            }
            for bound in bounds {
                if bound.ident == *target_ident {
                    return true;
                }
            }
            false
        }
        _ => false, // 可根据需要实现
    }
}

/// 构建泛型参数列表（生命周期和类型参数），自动添加逗号分隔符
fn build_generic_params(
    lifetimes: &[Lifetime],
    type_params: &[Ident],
) -> proc_macro2::TokenStream {
    let mut ts = proc_macro2::TokenStream::new();
    let mut first = true;
    for lt in lifetimes {
        if !first {
            ts.extend(quote! { , });
        }
        first = false;
        ts.extend(quote! { #lt });
    }
    for ty in type_params {
        if !first {
            ts.extend(quote! { , });
        }
        first = false;
        ts.extend(quote! { #ty });
    }
    ts
}

/// 递归地将类型中所有引用生命周期替换为 `new_lt`
fn transform_type_outer_lifetime(ty: &Type, new_lt: &Lifetime) -> Type {
    match ty {
        Type::Reference(ty_ref) => {
            // 处理最外层引用：替换生命周期，保持 mut 属性
            let mut new_ref = ty_ref.clone();
            new_ref.lifetime = Some(new_lt.clone());
            // 递归处理内部的元素类型（将内层引用生命周期变为匿名）
            let inner_transformed =
                transform_type_outer_lifetime(&ty_ref.elem, new_lt);
            new_ref.elem = Box::new(inner_transformed);
            Type::Reference(new_ref)
        }
        // 其他复合类型（元组、数组、切片等）需要递归内部元素
        Type::Tuple(tuple) => {
            let new_elems = tuple
                .elems
                .iter()
                .map(|elem| transform_type_outer_lifetime(elem, new_lt))
                .collect();
            Type::Tuple(syn::TypeTuple {
                paren_token: tuple.paren_token,
                elems: new_elems,
            })
        }
        Type::Array(arr) => {
            let new_elem = transform_type_outer_lifetime(&arr.elem, new_lt);
            Type::Array(TypeArray {
                bracket_token: arr.bracket_token,
                elem: Box::new(new_elem),
                len: arr.len.clone(),
                semi_token: arr.semi_token,
            })
        }
        Type::Slice(slice) => {
            let new_elem = transform_type_outer_lifetime(&slice.elem, new_lt);
            Type::Slice(syn::TypeSlice {
                bracket_token: slice.bracket_token,
                elem: Box::new(new_elem),
            })
        }
        Type::Paren(paren) => {
            let new_inner = transform_type_outer_lifetime(&paren.elem, new_lt);
            Type::Paren(syn::TypeParen {
                paren_token: paren.paren_token,
                elem: Box::new(new_inner),
            })
        }
        Type::Group(group) => {
            let new_elem = transform_type_outer_lifetime(&group.elem, new_lt);
            Type::Group(syn::TypeGroup {
                group_token: group.group_token,
                elem: Box::new(new_elem),
            })
        }
        Type::Path(type_path) => {
            // 处理路径类型，需要递归修改泛型参数中的生命周期
            let mut new_path = type_path.clone();
            // 对每个路径段，处理其泛型参数
            #[allow(clippy::single_match)]
            for seg in &mut new_path.path.segments {
                match &mut seg.arguments {
                    PathArguments::AngleBracketed(args) => {
                        let mut new_args = Punctuated::new();
                        for arg in &args.args {
                            let new_arg = match arg {
                                GenericArgument::Lifetime(lt) => {
                                    // 保留路径泛型参数中的生命周期原样输出，
                                    // 不再强行统一成 new_lt。
                                    GenericArgument::Lifetime(lt.clone())
                                }
                                GenericArgument::Type(ty) => {
                                    let transformed_ty =
                                        transform_type_outer_lifetime(
                                            ty, new_lt,
                                        );
                                    GenericArgument::Type(transformed_ty)
                                }
                                other => other.clone(),
                            };
                            new_args.push(new_arg);
                        }
                        args.args = new_args;
                    }
                    // PathArguments::Parenthesized(args) => {
                    //     // 类似地处理 Fn 语法中的参数和返回值
                    //     let mut new_inputs = Punctuated::new();
                    //     for input in &args.inputs {
                    //         let transformed = transform_type_outer_lifetime(input, new_lt);
                    //         new_inputs.push(transformed);
                    //     }
                    //     args.inputs = new_inputs;
                    //     if let Some(output) = &args.output {
                    //         let (arrow, ty) = output;
                    //         let transformed_ty = transform_type_outer_lifetime(ty, new_lt);
                    //         args.output = Some((arrow.clone(), Box::new(transformed_ty)));
                    //     }
                    // }
                    // PathArguments::None => {}
                    _ => {}
                }
            }
            Type::Path(new_path)
        }
        // 其他非复合类型（路径、原始指针等）不改变
        _ => ty.clone(),
    }
}

/// 将返回类型中由用户声明的生命周期（`user_lifetimes` 中除 `last_lt` 之外的生命周期）
/// 统一替换为 `last_lt`，使得返回类型能够在只携带 `last_lt` 一个生命周期参数的
/// 生成类型/impl 中表达出来。
///
/// 与 [`transform_type_outer_lifetime`] 不同，这里不会改动 `'static`、`'_`
/// 以及省略（匿名）的生命周期：`-> &'static str` 之类的返回类型必须原样保留，
/// 否则会把实际输出 `&'static str` 与声称的输出类型弄得不一致。
fn transform_output_lifetimes(
    ty: &Type,
    user_lifetimes: &[Lifetime],
    last_lt: &Lifetime,
) -> Type {
    fn repl(
        ty: &Type,
        user_lifetimes: &[Lifetime],
        last_lt: &Lifetime,
    ) -> Type {
        let is_user_lt = |lt: &Lifetime| {
            lt.ident != last_lt.ident
                && user_lifetimes.iter().any(|u| u.ident == lt.ident)
        };
        match ty {
            Type::Reference(ty_ref) => {
                let mut new_ref = ty_ref.clone();
                if let Option::Some(lt) = &ty_ref.lifetime
                    && is_user_lt(lt)
                {
                    new_ref.lifetime = Option::Some(last_lt.clone());
                }
                new_ref.elem =
                    Box::new(repl(&ty_ref.elem, user_lifetimes, last_lt));
                Type::Reference(new_ref)
            }
            Type::Tuple(tuple) => {
                let new_elems = tuple
                    .elems
                    .iter()
                    .map(|elem| repl(elem, user_lifetimes, last_lt))
                    .collect();
                Type::Tuple(syn::TypeTuple {
                    paren_token: tuple.paren_token,
                    elems: new_elems,
                })
            }
            Type::Array(arr) => {
                let new_elem = repl(&arr.elem, user_lifetimes, last_lt);
                Type::Array(TypeArray {
                    bracket_token: arr.bracket_token,
                    elem: Box::new(new_elem),
                    len: arr.len.clone(),
                    semi_token: arr.semi_token,
                })
            }
            Type::Slice(slice) => {
                let new_elem = repl(&slice.elem, user_lifetimes, last_lt);
                Type::Slice(syn::TypeSlice {
                    bracket_token: slice.bracket_token,
                    elem: Box::new(new_elem),
                })
            }
            Type::Paren(paren) => {
                let new_inner = repl(&paren.elem, user_lifetimes, last_lt);
                Type::Paren(syn::TypeParen {
                    paren_token: paren.paren_token,
                    elem: Box::new(new_inner),
                })
            }
            Type::Group(group) => {
                let new_elem = repl(&group.elem, user_lifetimes, last_lt);
                Type::Group(syn::TypeGroup {
                    group_token: group.group_token,
                    elem: Box::new(new_elem),
                })
            }
            Type::Path(type_path) => {
                let mut new_path = type_path.clone();
                for seg in &mut new_path.path.segments {
                    if let PathArguments::AngleBracketed(args) =
                        &mut seg.arguments
                    {
                        let mut new_args = Punctuated::new();
                        for arg in &args.args {
                            let new_arg = match arg {
                                GenericArgument::Lifetime(lt)
                                    if is_user_lt(lt) =>
                                {
                                    GenericArgument::Lifetime(last_lt.clone())
                                }
                                GenericArgument::Type(ty) => {
                                    GenericArgument::Type(repl(
                                        ty,
                                        user_lifetimes,
                                        last_lt,
                                    ))
                                }
                                other => other.clone(),
                            };
                            new_args.push(new_arg);
                        }
                        args.args = new_args;
                    }
                }
                Type::Path(new_path)
            }
            // 其他非复合类型（原始指针、函数指针等）不改变
            _ => ty.clone(),
        }
    }
    repl(ty, user_lifetimes, last_lt)
}
