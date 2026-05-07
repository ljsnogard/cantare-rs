use syn::{
    parse_macro_input,
    parse_quote,
    punctuated::Punctuated,
    AngleBracketedGenericArguments,
    FnArg, GenericParam, GenericArgument, Ident, ItemFn,
    Lifetime, PredicateLifetime, PredicateType,
    PatType, Path, PathArguments,
    Token, Type, TypeArray, TypePath, TypeParamBound, TraitBound,
    WhereClause, WherePredicate,
};
use proc_macro::TokenStream;
use quote::{format_ident, quote};

#[proc_macro_attribute]
pub fn gen_may_cancel_future(attr: TokenStream, item: TokenStream) -> TokenStream {
    let prefix_args = parse_macro_input!(attr with Punctuated::<Path, Token![,]>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);

    // 要生成的各个 struct 的名称前缀，在调用宏时在代码中指定
    let prefix_ident = if prefix_args.len() == 1 {
        prefix_args.first().unwrap().get_ident().cloned().expect("Expected identifier as path")
    } else {
        panic!("Expected exactly one identifier as prefix");
    };

    // 检查输入的函数是否有 async 修饰
    if input_fn.sig.asyncness.is_none() {
        panic!("`#[gen_may_cancel_future]` can only be applied to async functions");
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
            .filter(|pred|
                !predicate_contains_type_param(pred, &cancel_type_param)
                    && !predicate_contains_lifetime(pred, &last_lt)
            )
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
                    if let Type::Path(TypePath { qself: None, path }) = &**ty {
                        let Option::Some(last_seg) = path.segments.last() else {
                            panic!("Last argument check: must be Pin<&mut C>");
                        };
                        if last_seg.ident != "Pin" {
                            panic!("Last argument check: Pin");
                        }
                        let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) = &last_seg.arguments else {
                            panic!("Last argument check: AngleBracketed(AngleBracketedGenericArguments) ")
                        };
                        if args.len() != 1 {
                            panic!("Last argument check: Pin type generic args count")
                        }
                        let GenericArgument::Type(Type::Reference(cancel_type_ref)) = &args[0] else {
                            panic!("Last argument check: Pin type generic args content")
                        };
                        if cancel_type_ref.mutability.is_none() {
                            panic!("Last argument check: mut not found");
                        }
                        let Option::Some(lt_arg) = cancel_type_ref.lifetime.as_ref() else {
                            panic!("Last argument check: lifetime missing");
                        };
                        if lt_arg.ident != last_lt.ident {
                            panic!("Last argument check: lifetime of cancellation token must be the last one");
                        }
                        let Type::Path(generic_cancel_type_path) = cancel_type_ref.elem.as_ref() else {
                            panic!("Last argument check: cancel token type must be simple type token");
                        };
                        if generic_cancel_type_path.path.segments.len() != 1 {
                            panic!("Last argument check: cancel token type should be generic type");
                        }
                        let cancel_tok_type_ident = &generic_cancel_type_path.path.segments[0].ident;
                        if !generics_all.contains(cancel_tok_type_ident) {
                            panic!("Last argument check: cancel token type mismatch");
                        }
                    }
                    cancel_type = Option::Some(ty.clone());
                    // cancel_pat = Some(pat.clone());
                } else {
                    let orig_ty = ty.clone();
                    // 转换外层引用生命周期
                    let transformed_ty = transform_type_outer_lifetime(&orig_ty, &last_lt);
                    fields.push(transformed_ty.clone());
                    types.push(transformed_ty);
                    args.push(pat.clone());
                }
            }
            _ => panic!("Unsupported argument format"),
        }
    }

    let field_indices: Vec<syn::Index> = (0..args.len()).map(syn::Index::from).collect();

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

    let unified_lt_vec = vec![last_lt.clone()];
    let generic_params_single_lt_no_cancel = build_generic_params(&unified_lt_vec, &generics_no_cancel);
    let generic_params_single_lt_all = build_generic_params(&unified_lt_vec, &generics_all);

    let cancel_type_lt_replaced = transform_type_outer_lifetime(cancel_type.as_ref().unwrap(), &last_lt);

    let expanded = quote! {
        // panic!("input_fn 是: {:#?}", input_fn);
        #input_fn
        // panic!("lt_no_last 结构是: {:#?}\ngenerics_no_cancel 结构是: {:#?}", lt_no_last, generics_no_cancel);
        pub struct #async_struct<#generic_params_single_lt_no_cancel>(#(#fields),*)
        #where_clause_no_cancel_no_lt;

        pub struct #future_struct<#generic_params_single_lt_all>
        #where_clause_no_lt
        {
            params_: #async_struct<#generic_params_single_lt_no_cancel>,
            cancel_: #cancel_type_lt_replaced,
            future_: Option<<#state_struct<#generic_params_single_lt_all> as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture>,
        }

        // Declair #state_struct
        struct #state_struct<#generic_params_single_lt_all>(::core::pin::Pin<&#last_lt mut #future_struct<#generic_params_single_lt_all>>)
        #where_clause_no_lt;

        // Implement `IntoFuture` for #async_struct
        impl<#generic_params_single_lt_no_cancel> ::core::future::IntoFuture for #async_struct<#generic_params_single_lt_no_cancel>
        #where_clause_no_cancel_no_lt
        {
            type IntoFuture = #future_struct<#generic_params_single_lt_no_cancel, abs_sync::cancellation::NonCancellableToken>;
            type Output = #output_ty;

            fn into_future(self) -> Self::IntoFuture {
                #future_struct {
                    params_: self,
                    cancel_: abs_sync::cancellation::NonCancellableToken::shared_pin(),
                    future_: Option::None,
                }
            }
        }

        // Implement `TrMayCancel<'a>` for #async_struct
        impl<#generic_params_single_lt_no_cancel> abs_sync::may_cancel::TrMayCancel<#last_lt> for #async_struct<#generic_params_single_lt_no_cancel>
        #where_clause_no_cancel_no_lt
        {
            type MayCancelOutput = #output_ty;

            fn may_cancel_with<'cancel_, C: abs_sync::cancellation::TrCancellationToken>(
                self,
                cancel: ::core::pin::Pin<&'cancel_ mut C>,
            ) -> impl ::core::future::IntoFuture<Output = Self::MayCancelOutput>
            where
                Self: 'cancel_,
            {
                #future_struct {
                    params_: self,
                    cancel_: cancel,
                    future_: Option::None,
                }
            }
        }

        // Implement `Future` for #future_struct
        impl<#generic_params_single_lt_all> ::core::future::Future for #future_struct<#generic_params_single_lt_all>
        #where_clause_no_lt
        {
            type Output = #output_ty;

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

        impl<#generic_params_single_lt_all> ::core::ops::AsyncFnOnce<()> for #state_struct<#generic_params_single_lt_all>
        #where_clause_no_lt
        {
            type Output = #output_ty;
            type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;

            extern "rust-call" fn async_call_once(self, _: ()) -> Self::CallOnceFuture {
                let f = unsafe { self.0.get_unchecked_mut() };
                let p = &mut f.params_;
                self::#fn_ident(#(p.#field_indices),*, f.cancel_.as_mut())
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
                && lt.ident == target_lt.ident {
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
                                if lt.ident == target_lt.ident => {
                                    return true;
                                }
                            GenericArgument::Type(ty)
                                if ty_contains_lifetime(ty, target_lt) => {
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

/// 判断一个 WherePredicate 是否包含指定的生命周期
fn predicate_contains_lifetime(pred: &WherePredicate, target_lt: &Lifetime) -> bool {
    match pred {
        WherePredicate::Lifetime(PredicateLifetime { lifetime, bounds , ..}) => {
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
        WherePredicate::Type(PredicateType { bounded_ty, bounds, .. }) => {
            if ty_contains_lifetime(bounded_ty, target_lt) {
                return true;
            }
            for bound in bounds {
                match bound {
                    TypeParamBound::Lifetime(lt)
                        if lt.ident == target_lt.ident => {
                            return true;
                        }
                    TypeParamBound::Trait(TraitBound { path, .. }) => {
                        // 检查 trait 路径中是否包含目标生命周期
                        for seg in &path.segments {
                            if let PathArguments::AngleBracketed(args) = &seg.arguments {
                                for arg in &args.args {
                                    if let GenericArgument::Lifetime(lt) = arg && lt.ident == target_lt.ident {
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
                && seg.ident == *target_ident {
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
        Type::Reference(ty_ref) => ty_contains_type_param(&ty_ref.elem, target_ident),
        Type::Slice(ty_slice) => ty_contains_type_param(&ty_slice.elem, target_ident),
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
fn predicate_contains_type_param(pred: &WherePredicate, target_ident: &Ident) -> bool {
    match pred {
        WherePredicate::Type(PredicateType { bounded_ty, bounds, .. }) => {
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
                            if let PathArguments::AngleBracketed(args) = &seg.arguments {
                                for arg in &args.args {
                                    if let GenericArgument::Type(ty) = arg
                                        && ty_contains_type_param(ty, target_ident) {
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
        WherePredicate::Lifetime(PredicateLifetime { lifetime, bounds, .. }) => {
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
fn build_generic_params(lifetimes: &[Lifetime], type_params: &[Ident]) -> proc_macro2::TokenStream {
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
            let inner_transformed = transform_type_outer_lifetime(&ty_ref.elem, new_lt);
            new_ref.elem = Box::new(inner_transformed);
            Type::Reference(new_ref)
        }
        // 其他复合类型（元组、数组、切片等）需要递归内部元素
        Type::Tuple(tuple) => {
            let new_elems = tuple.elems.iter()
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
                                GenericArgument::Lifetime(_) => {
                                    // 将普通的生命周期参数替换为匿名生命周期
                                    GenericArgument::Lifetime(new_lt.clone())
                                }
                                GenericArgument::Type(ty) => {
                                    let transformed_ty = transform_type_outer_lifetime(ty, new_lt);
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
