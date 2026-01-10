// Build script for atl-server
// Generates Rust code from proto/sequencer.proto when grpc feature is enabled

fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        tonic_build::compile_protos("proto/sequencer.proto")?;
    }
    Ok(())
}
