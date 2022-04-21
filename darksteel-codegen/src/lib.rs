use convert_case::{Case, Casing};
use lazy_static::lazy_static;
use proc_macro::TokenStream;
use proc_macro_error::{abort, proc_macro_error};
use quote::{format_ident, quote, ToTokens};
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Mutex;
use std::{collections::hash_map::DefaultHasher, sync::Arc};
use syn::{
    parse_macro_input, spanned::Spanned, AttributeArgs, FnArg, Ident, ImplItem, ItemImpl,
    ItemStruct, Lit, NestedMeta, Pat, PatType, ReturnType, Type,
};

lazy_static! {
    static ref IDENTITIES: Arc<Mutex<HashMap<u64, String>>> = Default::default();
}

#[proc_macro_attribute]
#[proc_macro_error]
/// Easily implement a distributed state on a struct implementation.
pub fn distributed(_: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemImpl);
    let generics = input.generics.clone();
    let full_name = if let Type::Path(name) = *input.self_ty.clone() {
        name.path.segments.clone()
    } else {
        abort!(
            input.self_ty,
            "Can only be implemented on a qualified type."
        );
    };
    let name = if let Some(name) = full_name.pairs().last() {
        name.value().ident.clone()
    } else {
        abort!(input.self_ty, "Qualified path is invalid.");
    };
    let enum_name = format_ident!("{}Function", name);

    if input.trait_ != None {
        abort!(
            input,
            "Distributed implementations cannot be made from a trait."
        );
    }

    if generics.lifetimes().count() > 0 {
        abort!(
            generics,
            "Distributed implementations should not have any lifetimes."
        );
    }

    let enum_variants = input.items.iter().filter_map(|item| match item {
        ImplItem::Method(method) => {
            if !method.sig.inputs.pairs().any(|pair| match pair.value() {
                FnArg::Receiver(_) => true,
                _ => false,
            }) {
                abort!(
                    method,
                    "Distributed functions must take `self` as a parameter."
                )
            }

            if method.sig.generics.lifetimes().count() > 0
                && method.sig.generics.type_params().count() > 0
            {
                abort!(
                    method.sig.generics,
                    "Distributed functions cannot have lifetimes or generic parameters."
                );
            }

            if method.sig.output != ReturnType::Default {
                abort!(
                    method.sig.output,
                    "Distributed functions cannot have a return type."
                );
            }

            let ident = Ident::new(
                &method
                    .sig
                    .ident
                    .clone()
                    .to_string()
                    .to_case(Case::UpperCamel),
                method.sig.ident.span(),
            );

            let args: Vec<PatType> = method
                .sig
                .inputs
                .pairs()
                .filter_map(|pair| match pair.value() {
                    FnArg::Receiver(_) => None,
                    FnArg::Typed(arg) => {
                        if let Type::Reference(_) = *arg.ty {
                            abort!(
                                arg.ty,
                                "Distributed functions cannot take references as arguments."
                            )
                        }
                        Some(arg.clone())
                    }
                })
                .collect();

            if args.len() > 0 {
                Some(quote! {
                    #ident { #(#args),* }
                })
            } else {
                Some(ident.to_token_stream())
            }
        }
        _ => None,
    });

    let create_variants = input.items.iter().filter_map(|item| match item {
        ImplItem::Method(method) => {
            let function_ident = method.sig.ident.clone();
            let enum_ident = Ident::new(
                &function_ident.to_string().to_case(Case::UpperCamel),
                method.sig.ident.span(),
            );
            let signature: Vec<PatType> = method.sig.inputs
                .clone()
                .pairs()
                .filter_map(|pair| match pair.value() {
                    FnArg::Receiver(_) => None,
                    FnArg::Typed(argument) => Some(argument.clone())
                })
                .collect();

            let args: Vec<Ident> = method
                .sig
                .inputs
                .pairs()
                .filter_map(|pair| match pair.value() {
                    FnArg::Receiver(_) => None,
                    FnArg::Typed(argument) => match *argument.pat.clone() {
                        Pat::Ident(pat_ident) => Some(pat_ident.ident),
                        _ => unreachable!(),
                    },
                })
                .collect();

            if args.len() > 0 {
                Some(quote! {
                    #[inline(always)]
                    fn #function_ident (#(#signature),*) -> #enum_name { #enum_name :: #enum_ident { #(#args),* } }
                })
            } else {
                Some(quote! {
                    #[inline(always)]
                    fn #function_ident () -> #enum_name { #enum_name :: #enum_ident }
                })
            }
        }
        _ => None,
    });

    let match_variants = input.items.iter().filter_map(|item| match item {
        ImplItem::Method(method) => {
            let function_ident = method.sig.ident.clone();
            let function_real_ident = format_ident!("_{}", function_ident);
            let enum_ident = Ident::new(
                &function_ident.to_string().to_case(Case::UpperCamel),
                method.sig.ident.span(),
            );

            let args: Vec<Ident> = method
                .sig
                .inputs
                .pairs()
                .filter_map(|pair| match pair.value() {
                    FnArg::Receiver(_) => None,
                    FnArg::Typed(argument) => match *argument.pat.clone() {
                        Pat::Ident(pat_ident) => Some(pat_ident.ident),
                        _ => unreachable!(),
                    },
                })
                .collect();

            // `store` is the variable in the `Mutator` trait
            if args.len() > 0 {
                Some(quote! {
                    #enum_name :: #enum_ident { #(#args),* } => self.#function_real_ident (#(#args),*)
                })
            } else {
                Some(quote! {
                    #enum_name :: #enum_ident => self.#function_real_ident ()
                })
            }
        }
        _ => None,
    });

    let mut input = input.clone();

    for item in &mut input.items {
        match item {
            ImplItem::Method(method) => {
                method.sig.ident = format_ident!("_{}", method.sig.ident);
            }
            _ => (),
        }
    }

    let expanded = quote! {
        #input

        #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
        pub enum #enum_name #generics {
            #(#enum_variants),*
        }

        impl #full_name {
            #(#create_variants),*
        }

        #[typetag::serde]
        impl darksteel::modules::distributed::DistributedState for #full_name {
            fn mutate(&mut self, data: Vec<u8>) -> Result<Vec<u8>, darksteel::modules::distributed::error::MutatorError> {
                let mutation: #enum_name = bincode::deserialize(data.as_slice())?;

                match mutation {
                    #(#match_variants),*
                }

                Ok(bincode::serialize(self)?)
            }
        }

        impl #generics darksteel::modules::distributed::Mutator for #enum_name #generics {
            #[inline(always)]
            fn state_id() -> darksteel::identity::Identity {
                #full_name ::ID
            }

            #[inline(always)]
            fn bytes(&self) -> Result<Vec<u8>, darksteel::modules::distributed::error::MutatorError> {
                Ok(bincode::serialize(self)?)
            }
        }
    };

    let tokens = TokenStream::from(expanded);

    //panic!(tokens.to_string());

    tokens
}

#[proc_macro_attribute]
#[proc_macro_error]
/// A convenience function for implementing an identity trait.
pub fn identity(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let arguments = parse_macro_input!(arguments as AttributeArgs);
    let ident = input.ident.clone();
    let generics = input.generics.clone();

    if arguments.len() < 1 {
        abort!(
            input,
            "`#[darksteel::identity]` requires at least one argument"
        );
    }

    let concatenated =
        arguments
            .iter()
            .fold(String::new(), |mut string, argument| match argument {
                NestedMeta::Meta(meta) => {
                    abort!(meta.span(), "Argument must be string literal");
                }
                NestedMeta::Lit(literal) => match literal {
                    Lit::Str(literal) => {
                        string += literal.value().as_str();
                        if arguments.last().unwrap() != argument {
                            string += ", ";
                        }
                        string
                    }
                    _ => abort!(literal.span(), "Argument must be string literal"),
                },
            });

    let mut identities = IDENTITIES.lock().expect("Identities lock poisoned");
    // TODO: replace this with a specific hasher as defaults can change over time.
    let mut hasher = DefaultHasher::new();
    hasher.write(concatenated.as_bytes());
    let id = hasher.finish();

    if identities.contains_key(&id) {
        abort!(
            input, "Identity already exists";
            help = "Try renaming the identity for attribute macro: `#[darksteel::identity(\"{}\")]`", concatenated
        );
    }
    let id_token = id.into_token_stream();
    let expanded = quote! {
        #input

        impl #generics darksteel::identity::IdentityTrait for #ident #generics {
            const ID: darksteel::identity::Identity = #id_token;
        }
    };
    identities.insert(id, ident.to_string());
    let tokens = TokenStream::from(expanded);

    //panic!(tokens.to_string());

    tokens
}
