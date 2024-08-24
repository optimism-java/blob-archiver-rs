mod api;

#[allow(dead_code)]
static INIT: std::sync::Once = std::sync::Once::new();

use clap::Parser;

fn main() {
    println!("Hello, world!");
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CliArgs {
    #[clap(short, long, value_parser, default_value = "config.toml")]
    config: String,

    #[clap(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(short, long)]
    dry_run: bool,
}
