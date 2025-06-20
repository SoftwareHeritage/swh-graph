// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

const PROTO_PATH: &str = "./proto/swhgraph.proto";
const PROTO_DIR: &str = "./proto/";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR").expect("Missing OUT_DIR");
    let out_dir = std::path::Path::new(&out_dir);

    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("swhgraph_descriptor.bin"))
        .protoc_arg("--experimental_allow_proto3_optional") // Needed on Debian 11
        .compile_protos(&[PROTO_PATH], &[PROTO_DIR])?;
    Ok(())
}
