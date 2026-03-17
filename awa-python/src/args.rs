use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::job::py_to_json;

/// Derive the kind string from a Python class name using the same algorithm as Rust.
///
/// CamelCase → snake_case per PRD §9.2.
pub fn derive_kind(class_name: &str) -> String {
    let mut result = String::with_capacity(class_name.len() + 4);
    let chars: Vec<char> = class_name.chars().collect();

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

/// Serialize a Python args object (dataclass or pydantic BaseModel) to JSON.
///
/// Detection: pydantic has `model_dump`, dataclass has `__dataclass_fields__`.
pub fn serialize_args(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    // Check for pydantic BaseModel (has model_dump)
    if let Ok(dump_fn) = obj.getattr("model_dump") {
        let kwargs = PyDict::new(py);
        kwargs.set_item("mode", "json")?;
        let dumped = dump_fn.call((), Some(&kwargs))?;
        return py_to_json(py, &dumped);
    }

    // Check for dataclass (has __dataclass_fields__)
    if obj.hasattr("__dataclass_fields__")? {
        let asdict = py.import("dataclasses")?.getattr("asdict")?;
        let dict = asdict.call1((obj,))?;
        return py_to_json(py, &dict);
    }

    // Check if it's already a dict
    if let Ok(dict) = obj.cast::<PyDict>() {
        return py_to_json(py, dict.as_any());
    }

    Err(pyo3::exceptions::PyTypeError::new_err(
        "Job args must be a dataclass, pydantic BaseModel, or dict",
    ))
}

/// Get the class name from a Python args object.
pub fn get_class_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    obj.get_type().qualname().map(|s| s.to_string())
}

/// Get the class name from a Python type.
pub fn get_type_class_name(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    obj.getattr("__name__")?.extract::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_kind_golden_cases() {
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
            let result = derive_kind(input);
            assert_eq!(
                result, expected,
                "derive_kind({input:?}): expected {expected:?}, got {result:?}"
            );
        }
    }
}
