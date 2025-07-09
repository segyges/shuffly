use clap::Parser;
use shuffly::sum_numbers;

#[derive(Parser)]
#[command(name = "shuffly")]
struct Cli {
    /// First number
    a: usize,
    /// Second number  
    b: usize,
}

fn main() {
    let cli = Cli::parse();
    let result = sum_numbers(cli.a, cli.b);
    println!("{}", result);
}