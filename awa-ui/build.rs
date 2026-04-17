//! Cache-invalidation bridge for `rust-embed` + `static/`.
//!
//! `#[derive(Embed)] #[folder = "static/"]` emits `include_bytes!` per file,
//! so Cargo invalidates when an existing embedded file's contents change. It
//! does not invalidate when files are added or removed under the folder: the
//! derive macro's output is keyed off `src/lib.rs` and the crate's declared
//! inputs, not off the directory listing.
//!
//! This matters with `actions/cache` / `Swatinem/rust-cache`: a restored
//! `target/` from a prior build whose Vite output produced different
//! hash-stamped `assets/index-<hash>.{js,css}` filenames would reuse the old
//! compiled binary and serve stale embedded assets. The HTML would point at
//! asset paths the embedded filesystem no longer contains, and a smoke test
//! that only greps for `/assets/` would not catch the mismatch.
//!
//! Watching `static/` and `static/assets/` invalidates this crate whenever a
//! file is added or removed (directory mtime changes). File-content changes
//! are already covered by `include_bytes!` fingerprinting in the generated
//! code, so individual files do not need to be enumerated here.

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR is always set during build scripts");
    println!("cargo:rerun-if-changed={manifest_dir}/static");
    println!("cargo:rerun-if-changed={manifest_dir}/static/assets");
}
