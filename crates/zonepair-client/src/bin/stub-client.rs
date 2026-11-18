//! stub-client: thin shell over `zonepair_client::cli::main` (machine A).

fn main() -> anyhow::Result<()> {
    zonepair_client::cli::main()
}
