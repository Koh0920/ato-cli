fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/tsnet")
        .compile(&["proto/tsnet/v1/tsnet.proto"], &["proto"])?;
    Ok(())
}
