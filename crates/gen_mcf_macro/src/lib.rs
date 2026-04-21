use syn::{
    parse_macro_input,
    parse_quote,
    punctuated::Punctuated,
    AngleBracketedGenericArguments,
    FnArg, GenericParam, GenericArgument, ItemFn, PatType, Path, PathArguments,
    Token, Type, TypePath, WhereClause,
};
use proc_macro::TokenStream;
use quote::{format_ident, quote};

#[proc_macro_attribute]
pub fn gen_may_cancel_future(attr: TokenStream, item: TokenStream) -> TokenStream {
    let prefix_args = parse_macro_input!(attr with Punctuated::<Path, Token![,]>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);

    let prefix_ident = if prefix_args.len() == 1 {
        prefix_args.first().unwrap().get_ident().cloned().expect("Expected identifier as path")
    } else {
        panic!("Expected exactly one identifier as prefix");
    };

    // Check function is async
    if input_fn.sig.asyncness.is_none() {
        panic!("`#[gen_may_cancel_future]` can only be applied to async functions");
    }

    // Extract signature parts
    // let vis = &input_fn.vis;
    let fn_ident = &input_fn.sig.ident;
    let fn_generics = &input_fn.sig.generics;
    let Option::Some(where_clause) = &input_fn.sig.generics.where_clause else {
        panic!("Function must have where clause for generics");
    };
    let where_clause_no_cancel = {
        let mut preds_no_cancel = where_clause.predicates.clone();
        let take_len = preds_no_cancel.len().saturating_sub(1);
        preds_no_cancel = preds_no_cancel
            .into_pairs()
            .take(take_len)
            .collect();
        if !preds_no_cancel.is_empty() {
            WhereClause {
                where_token: where_clause.where_token,
                predicates: preds_no_cancel,
            }
        } else {
            // A dummy where clause
            parse_quote! {
                where 'static: 'static
            }
        }
    };

    let sig_inputs = &input_fn.sig.inputs;
    let sig_output = &input_fn.sig.output;

    // Extract lifetime
    let mut lifetimes = vec![];
    for param in &fn_generics.params {
        if let GenericParam::Lifetime(lt) = param {
            lifetimes.push(lt.lifetime.clone());
        }
    }
    if lifetimes.len() != 1 {
        panic!("Function must have exactly one named lifetime");
    }
    let lt = &lifetimes[0]; // e.g., 'f

    // Extract all generic type parameters
    let (generics_all, generics_no_cancel) = {
        let mut generics_all = vec![];
        let mut generics_no_cancel = vec![];
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
        }
        if generics_all.is_empty() {
            panic!("Function must have at least one generic parameter");
        }
        (generics_all, generics_no_cancel)
    };

    // Extract inputs except cancel token
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
                            panic!("Last argument check: must be Pin<&'f mut C>");
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
                        if lt_arg.ident != lt.ident {
                            panic!("Last argument check: lifetime must contain the only lifetime");
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
                    fields.push(ty.clone());
                    types.push(ty.clone());
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

    let expanded = quote! {
        #input_fn

        pub struct #async_struct<#lt, #(#generics_no_cancel),*>(#(#fields),*)
        #where_clause_no_cancel;

        pub struct #future_struct<#lt, #(#generics_all),*,>
        #where_clause
        {
            params_: #async_struct<#lt, #(#generics_no_cancel),*>,
            cancel_: #cancel_type,
            future_: Option<<#state_struct<#lt, #(#generics_all),*,> as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture>,
        }

        // Declair #state_struct
        struct #state_struct<#lt, #(#generics_all),*,>(::core::pin::Pin<&#lt mut #future_struct<#lt, #(#generics_all),*,>>)
        #where_clause;

        // Implement `IntoFuture` for #async_struct
        impl<#lt, #(#generics_no_cancel),*> ::core::future::IntoFuture for #async_struct<#lt, #(#generics_no_cancel),*>
        #where_clause_no_cancel
        {
            type IntoFuture = #future_struct<#lt, #(#generics_no_cancel),*, ::abs_sync::cancellation::NonCancellableToken>;
            type Output = #output_ty;

            fn into_future(self) -> Self::IntoFuture {
                #future_struct {
                    params_: self,
                    cancel_: ::abs_sync::cancellation::NonCancellableToken::shared_pin(),
                    future_: Option::None,
                }
            }
        }

        // Implement `TrMayCancel<'a>` for #async_struct
        impl<#lt, #(#generics_no_cancel),*> ::abs_sync::may_cancel::TrMayCancel<#lt> for #async_struct<#lt, #(#generics_no_cancel),*>
        #where_clause_no_cancel
        {
            type MayCancelOutput = #output_ty;

            fn may_cancel_with<'cancel_, C: ::abs_sync::cancellation::TrCancellationToken>(
                self,
                cancel: ::core::pin::Pin<&'cancel_ mut C>,
            ) -> impl ::core::future::IntoFuture<Output = Self::MayCancelOutput>
            where
                Self:'cancel_,
            {
                #future_struct {
                    params_: self,
                    cancel_: cancel,
                    future_: Option::None,
                }
            }
        }

        // Implement `Future` for #future_struct
        impl<#lt, #(#generics_all),*> ::core::future::Future for #future_struct<#lt, #(#generics_all),*>
        #where_clause
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

        impl<#(#generics_all),*> ::core::ops::AsyncFnOnce<()> for #state_struct<'_, #(#generics_all),*>
        #where_clause
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
