use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident, LitStr};

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
/// ```
#[proc_macro_derive(JobArgs, attributes(awa))]
pub fn derive_job_args(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Check for #[awa(kind = "custom")] attribute
    let custom_kind = input.attrs.iter().find_map(|attr| {
        if !attr.path().is_ident("awa") {
            return None;
        }
        let mut kind_value = None;
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("kind") {
                let value = meta.value()?;
                let s: LitStr = value.parse()?;
                kind_value = Some(s.value());
            }
            Ok(())
        });
        kind_value
    });

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

    let expanded = quote! {
        impl #model_path::JobArgs for #name {
            fn kind() -> &'static str {
                #kind_str
            }
        }
    };

    TokenStream::from(expanded)
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
}
