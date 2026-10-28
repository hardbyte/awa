use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident, LitStr, Type};

/// Derive macro for job argument types.
///
/// Generates the `JobArgs` trait implementation including:
/// - `kind()` — snake_case kind string (or custom override)
/// - Requires `Serialize + Deserialize` (user must derive those)
///
/// # Usage
///
/// ```ignore
/// use awa_model::JobArgs;
///
/// #[derive(Debug, Serialize, Deserialize, JobArgs)]
/// struct SendEmail {
///     pub to: String,
///     pub subject: String,
/// }
/// // kind() returns "send_email"
///
/// use awa_model::JobArgs;
///
/// #[derive(Debug, Serialize, Deserialize, JobArgs)]
/// #[awa(kind = "custom_kind")]
/// struct MyJob {
///     pub data: String,
/// }
/// // kind() returns "custom_kind"
///
/// // `#[awa(output = T)]` declares the type the handler records as its
/// // output (ADR-048), and implements `JobOutput` with `Output = T`.
/// #[derive(Debug, Serialize, Deserialize, JobArgs)]
/// #[awa(output = SyncSummary)]
/// struct SyncInventory {
///     pub feed: String,
/// }
/// ```
#[proc_macro_derive(JobArgs, attributes(awa))]
pub fn derive_job_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // #[awa(kind = "custom")] and #[awa(output = Type)]
    let mut custom_kind = None;
    let mut output_type: Option<Type> = None;
    for attr in input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("awa"))
    {
        let parsed = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                let s: LitStr = meta.value()?.parse()?;
                custom_kind = Some(s.value());
            } else if meta.path.is_ident("output") {
                output_type = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `kind` or `output`"));
            }
            Ok(())
        });
        if let Err(err) = parsed {
            return err.to_compile_error().into();
        }
    }

    let kind_str = custom_kind.unwrap_or_else(|| camel_to_snake(&name.to_string()));

    // Resolve the path to awa_model::JobArgs. Users may depend on either
    // the `awa` facade crate or `awa-model` directly. We check for `awa`
    // first (which re-exports awa_model), then fall back to `awa-model`.
    let model_path = if let Ok(found) = crate_name("awa") {
        // User depends on the `awa` facade which re-exports awa_model.
        let ident = match found {
            FoundCrate::Itself => Ident::new("awa", Span::call_site()),
            FoundCrate::Name(name) => Ident::new(&name, Span::call_site()),
        };
        quote!(::#ident::awa_model)
    } else if let Ok(found) = crate_name("awa-model") {
        // User depends on awa-model directly.
        match found {
            FoundCrate::Itself => quote!(crate),
            FoundCrate::Name(name) => {
                let ident = Ident::new(&name, Span::call_site());
                quote!(::#ident)
            }
        }
    } else {
        quote!(::awa_model)
    };

    let expanded = match output_type {
        None => quote! {
            impl #model_path::JobArgs for #name {
                fn kind() -> &'static str {
                    #kind_str
                }
            }
        },
        Some(output_type) => {
            let output_name = type_display_name(&output_type);
            quote! {
                impl #model_path::JobArgs for #name {
                    fn kind() -> &'static str {
                        #kind_str
                    }

                    fn output_type() -> ::core::option::Option<&'static str> {
                        ::core::option::Option::Some(#output_name)
                    }
                }

                impl #model_path::JobOutput for #name {
                    type Output = #output_type;
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// The name operators see for an output type: the last path segment with its
/// generic arguments, so `crate::sync::Summary<u32>` shows as `Summary<u32>`.
fn type_display_name(ty: &Type) -> String {
    let text = match ty {
        Type::Path(path) if path.qself.is_none() => path
            .path
            .segments
            .last()
            .map(|segment| quote!(#segment).to_string())
            .unwrap_or_else(|| quote!(#ty).to_string()),
        _ => quote!(#ty).to_string(),
    };
    text.replace(" < ", "<")
        .replace(" >", ">")
        .replace("< ", "<")
        .replace(" ,", ",")
}

/// Convert CamelCase to snake_case.
///
/// Algorithm (from PRD §9.2):
/// 1. Insert `_` before each uppercase letter following a lowercase letter or digit.
/// 2. Insert `_` before an uppercase letter followed by a lowercase letter, if preceded by uppercase.
/// 3. Lowercase everything.
fn camel_to_snake(name: &str) -> String {
    let mut result = String::with_capacity(name.len() + 4);
    let chars: Vec<char> = name.chars().collect();

    for i in 0..chars.len() {
        let current = chars[i];

        if current.is_uppercase() {
            let needs_separator = i > 0
                && ((chars[i - 1].is_lowercase() || chars[i - 1].is_ascii_digit())
                    || (chars[i - 1].is_uppercase()
                        && i + 1 < chars.len()
                        && chars[i + 1].is_lowercase()));
            if needs_separator {
                result.push('_');
            }
            result.push(current.to_lowercase().next().unwrap());
        } else {
            result.push(current);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden test cases from PRD §9.2.
    #[test]
    fn test_kind_derivation_golden_cases() {
        let cases = vec![
            ("SendEmail", "send_email"),
            ("SendConfirmationEmail", "send_confirmation_email"),
            ("SMTPEmail", "smtp_email"),
            ("OAuthRefresh", "o_auth_refresh"),
            ("PDFRenderJob", "pdf_render_job"),
            ("ProcessV2Import", "process_v2_import"),
            ("ReconcileQ3Revenue", "reconcile_q3_revenue"),
            ("HTMLToPDF", "html_to_pdf"),
            ("IOError", "io_error"),
        ];

        for (input, expected) in cases {
            let result = camel_to_snake(input);
            assert_eq!(
                result, expected,
                "camel_to_snake({input:?}): expected {expected:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_simple_cases() {
        assert_eq!(camel_to_snake("Job"), "job");
        assert_eq!(camel_to_snake("MyJob"), "my_job");
        assert_eq!(camel_to_snake("A"), "a");
        assert_eq!(camel_to_snake("AB"), "ab");
    }

    #[test]
    fn output_type_names_drop_the_path() {
        for (input, expected) in [
            ("Summary", "Summary"),
            ("crate::sync::Summary", "Summary"),
            ("Vec<u32>", "Vec<u32>"),
            ("report::Page<String, u64>", "Page<String, u64>"),
            ("(u32, String)", "(u32, String)"),
        ] {
            let ty: Type = syn::parse_str(input).unwrap();
            assert_eq!(type_display_name(&ty), expected, "{input}");
        }
    }
}
