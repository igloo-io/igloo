use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Igloo Query Engine CLI (Main Crate)");
    if let Some(config_path) = args.config {
        println!("Config file specified: {}", config_path);
        // Here you would load the configuration and start the appropriate service
        // e.g., coordinator, worker, or client based on the config or other args
    } else {
        println!("No config file specified. Starting in default mode or showing help.");
        // Potentially, print help or start a default component
    }

    // Example: use a function from the lib part of this crate
    println!("{}", igloo::hello());

    // Placeholder for actual application logic
    // Depending on the architecture, this binary might:
    // 1. Act as a client to a running Igloo cluster.
    // 2. Start a local single-node Igloo instance.
    // 3. Parse arguments to start as a coordinator or worker.

    Ok(())
}
