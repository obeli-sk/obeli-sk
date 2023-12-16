use std::{
    path::{Path, PathBuf},
    process::Command,
};

use cargo_metadata::camino::Utf8Path;

const BUILD_TARGET_TRIPPLE: &str = "wasm32-unknown-unknown";

pub fn build() {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let pkg_name = std::env::var("CARGO_PKG_NAME").unwrap();
    let pkg_name = pkg_name.strip_suffix("_builder").unwrap();
    let wasm = run_cargo_component_build(&out_dir, pkg_name);
    let mut generated_code = String::new();
    generated_code += &format!(
        "pub const {name_upper}: &str = {wasm:?};\n",
        name_upper = pkg_name.to_uppercase()
    );
    std::fs::write(out_dir.join("gen.rs"), generated_code).unwrap();

    let meta = cargo_metadata::MetadataCommand::new().exec().unwrap();
    let package = meta
        .packages
        .iter()
        .find(|p| p.name == pkg_name)
        .unwrap_or_else(|| panic!("package `{pkg_name}` must exist"));

    println!("cargo:rerun-if-changed={}", package.manifest_path); // Cargo.toml
    let mut src_paths: Vec<_> = package
        .targets
        .iter()
        .map(|target| target.src_path.parent().unwrap())
        .collect();
    let wit_path = &package.manifest_path.parent().unwrap().join("wit");
    if wit_path.exists() && wit_path.is_dir() {
        src_paths.push(wit_path);
    }
    for src_path in src_paths {
        add_dependency(src_path);
    }
}

fn add_dependency(file: &Utf8Path) {
    if file.is_file() {
        println!("cargo:rerun-if-changed={file}");
    } else {
        for file in file
            .read_dir_utf8()
            .unwrap_or_else(|err| panic!("cannot read folder `{file}` - {err:?}"))
            .flatten()
        {
            add_dependency(file.path())
        }
    }
}

fn run_cargo_component_build(out_dir: &Path, name: &str) -> PathBuf {
    let mut cmd = Command::new("cargo-component");
    cmd.arg("build")
        .arg("--release")
        .arg(format!("--target={BUILD_TARGET_TRIPPLE}"))
        .arg(format!("--package={name}"))
        .env("CARGO_TARGET_DIR", out_dir)
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    eprintln!("running: {cmd:?}");
    let status = cmd.status().unwrap();
    assert!(status.success());
    let target = out_dir
        .join(BUILD_TARGET_TRIPPLE)
        .join("release")
        .join(format!("{name}.wasm",));
    assert!(target.exists(), "Target path must exist: {target:?}");
    target
}