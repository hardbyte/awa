use axum::extract::{NestedPath, OriginalUri};
use axum::http::{header, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::Extension;
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "static/"]
struct StaticAssets;

/// Serve assets using the stripped URI, and the SPA using its original mount.
pub(crate) async fn static_handler(
    OriginalUri(original_uri): OriginalUri,
    uri: Uri,
    nested_path: Option<Extension<NestedPath>>,
) -> Response {
    let path = uri.path().trim_start_matches('/');

    // index.html is also an entry point, so it must get the same base as SPA
    // deep links rather than taking the unmodified static-file path.
    if path != "index.html" {
        if let Some(file) = StaticAssets::get(path) {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            return ([(header::CONTENT_TYPE, mime.as_ref())], file.data.to_vec()).into_response();
        }
    }

    let index = StaticAssets::get("index.html");
    let html = index
        .as_ref()
        .map(|file| String::from_utf8_lossy(&file.data))
        .unwrap_or_else(|| PLACEHOLDER_HTML.into());
    (
        [(header::CACHE_CONTROL, "no-cache")],
        Html(with_base(
            &html,
            mount_path(&original_uri, &uri),
            nested_path.is_some_and(|Extension(path)| path.as_str().ends_with('/')),
        )),
    )
        .into_response()
}

fn mount_path<'a>(original_uri: &'a Uri, uri: &Uri) -> &'a str {
    let original = original_uri.path();
    // Axum strips every outer nest prefix, but synthesizes "/" when the
    // request is exactly the mount point (e.g. GET /awa).
    if uri.path() == "/" {
        original.trim_end_matches('/')
    } else {
        original.strip_suffix(uri.path()).unwrap_or_default()
    }
}

fn with_base(html: &str, mount: &str, trailing_slash: bool) -> String {
    let mount = mount.trim_end_matches('/');
    let base = escape_attribute(&format!("{mount}/"));
    // Axum's nest("/awa", ...) exposes the root at /awa, while nest("/awa/",
    // ...) exposes /awa/. Assets always need a trailing slash in the base,
    // so give the client router the canonical root separately.
    let root = if mount.is_empty() || trailing_slash {
        format!("{mount}/")
    } else {
        mount.to_owned()
    };
    let root = escape_attribute(&root);
    // Both the Vite entry point and placeholder have this head marker. Put
    // the base first so the browser uses it for every following resource.
    html.replacen(
        "<head>",
        &format!("<head><base href=\"{base}\" data-awa-root=\"{root}\">"),
        1,
    )
}

fn escape_attribute(value: &str) -> String {
    // URI paths are not HTML: escape attributes, and encode backslashes so
    // browsers cannot interpret a leading /\ as a cross-origin URL.
    value
        .replace('\\', "%5C")
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

const PLACEHOLDER_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>AWA</title></head>
<body>
  <h1>AWA Job Queue</h1>
  <p>API available at <a href="api/stats">api/stats</a></p>
  <p>Frontend not built. Run <code>cd awa-ui/frontend && npm install && npm run build</code> to build the UI.</p>
</body>
</html>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_attribute_is_escaped() {
        let html = with_base("<head></head>", "/a\"&'<b>\\", false);
        assert_eq!(
            html,
            "<head><base href=\"/a&quot;&amp;&#39;&lt;b&gt;%5C/\" data-awa-root=\"/a&quot;&amp;&#39;&lt;b&gt;%5C\"></head>"
        );
    }
}
