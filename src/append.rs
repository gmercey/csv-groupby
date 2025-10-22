use std::error::Error;
use std::{
    io::{Write},
};

use bstr::io::BufReadExt;
use bstr::ByteSlice;
use std::time::Instant;
use clap::{Parser, ArgAction};

#[derive(Parser, Debug, Clone)]
#[command(author, about="appends or prefixes text to line from stdin", name="append")]
struct AppendCli {
    #[arg(short = 'p', long = "prefix_str", default_value = "")]
    /// string to prefix to stdout
    pub prefix_str: String,
    #[arg(short = 'a', long = "append_str", default_value = "")]
    /// string to append to stdout
    pub append_str: String,

    #[arg(short = 'v', action=ArgAction::Count)]
    /// Verbosity - use more than one v for greater detail
    pub verbose: usize,
}

mod gen;
use gen::{get_reader_writer};

fn main() {
    if let Err(err) = _main() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
    }

}

fn _main() -> Result<(), Box<dyn Error>> {
    let cli: AppendCli = AppendCli::parse();
    let start_f = Instant::now();

    let stdout = std::io::stdout();
    let _writerlock = stdout.lock();

    let (mut reader, mut writer) = get_reader_writer();
    reader.for_byte_line(|line| {
        if line.is_empty() {
            panic!("test entry from -L (line) was empty")
        }
        if !cli.prefix_str.is_empty() {
            writer.write_all(cli.prefix_str.as_bytes())?;
        }
        writer.write_all(line.as_bytes())?;
        if !cli.append_str.is_empty() {
            writer.write_all(cli.append_str.as_bytes())?;
        }
        writer.write_all(b"\n")?;
        Ok(true)
    })?;
    writer.flush()?;
    let end_f = Instant::now();
    if cli.verbose > 0 {
        eprintln!("runtime: {} secs", (end_f - start_f).as_secs());
    }
    Ok(())
}
