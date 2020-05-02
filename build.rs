fn main() {
    prost_build::compile_protos(&["proto/store_dictionary.proto"], &["proto/"]).unwrap();
}
