fn main() {
    // `links` makes Cargo enforce one metrics registry per dependency graph.
    println!("cargo::rerun-if-changed=build.rs");
}
