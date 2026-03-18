/// Convert CamelCase to snake_case for kind derivation.
///
/// NOTE: This is intentionally duplicated from `awa-macros` because proc-macro
/// crates cannot be used as library dependencies. Both copies are validated by
/// the same golden test suite (PRD §9.2) to ensure they stay in sync.
///
/// Algorithm (from PRD §9.2):
/// 1. Insert `_` before each uppercase letter following a lowercase letter or digit.
/// 2. Insert `_` before an uppercase letter followed by a lowercase letter, if preceded by uppercase.
/// 3. Lowercase everything.
pub fn camel_to_snake(name: &str) -> String {
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
}
