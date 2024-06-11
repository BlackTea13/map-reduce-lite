// Note(Appy): Currently every build target runs this build step. 
//             This is because of how Rust works. If we want to
//             seperate it out, we have to move each binary into their own
//             package. But given our starter code structure, we'll just 
//             have to live with this for now.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    
    // Compile the proto files via `prost`, generating service stubs
    // and proto definitions for use with `tonic`.
    tonic_build::compile_protos("protos/coordinator.proto")?;
    Ok(())
}
