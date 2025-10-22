use std::path::PathBuf;
use std::sync::Arc;
use std::collections::HashMap;

use pcre2::bytes::Regex;

use lazy_static::lazy_static;
use clap::{Parser, ArgAction};
use std::str::FromStr;
use std::sync::{RwLock, RwLockReadGuard};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use pcre2::bytes::{CaptureLocations as CaptureLocations_pcre2, Captures as Captures_pcre2, Regex as Regex_pre2};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn get_default_parse_thread_no() -> usize {
    if num_cpus::get() > 12 { 12 } else { num_cpus::get() }
}

fn get_default_io_thread_no() -> usize {
    if num_cpus::get() > 12 { 6 } else { num_cpus::get().div_ceil(2) }
}

fn get_default_queue_size() -> usize {
    get_default_parse_thread_no() * 4
}

// fn get_ver() -> String {
//     return format!("rev: {}", env!("RIPGREP_BUILD_GIT_HASH");
// }

lazy_static! {
    static ref DEFAULT_IO_THREAD_NO: String = get_default_io_thread_no().to_string();
    static ref DEFAULT_PARSE_THREAD_NO: String = get_default_parse_thread_no().to_string();
    static ref DEFAULT_QUEUE_SIZE: String = get_default_queue_size().to_string();
    pub static ref BUILD_INFO: String  = format!("  ver: {}  rev: {}",
        env!("CARGO_PKG_VERSION"), env!("BUILD_GIT_HASH"));
}

#[derive(Parser, Debug)]
#[command(version = BUILD_INFO.as_str(), rename_all = "kebab-case")]
/// Execute a sql-like group-by on arbitrary text or csv files. Field indices start at 1.
///
/// Note that -l, -f, and -i define where data comes from.  If none of these options
/// is given then it default to reading stdin.
pub struct CliCfg {
    #[arg(short='R', long="test_re")] pub testre: Option<String>,
    #[arg(short='L', long="test_line")] pub testlines: Vec<String>,
    // Overloaded key field selectors: numeric indices (1-based) OR header names.
    // Parsed initially as raw strings; resolved later (lazy) after header read.
    #[arg(short='k', long="key_fields", value_delimiter=',')] pub raw_key_fields: Vec<String>,
    // INTERNAL (initialized post-parse)
    #[clap(skip)] pub key_fields_idx: RwLock<Vec<usize>>,
    #[clap(skip)] pub key_field_names: Vec<String>,
    #[clap(skip)] pub key_fields_resolved: AtomicBool,
    #[clap(skip)] pub key_index_names: RwLock<HashMap<usize,String>>, // map of resolved index -> header name
    // Unique values (distinct counts) support numeric indices OR header names
    #[arg(short='u', long="unique_values", value_delimiter=',')] pub raw_unique_fields: Vec<String>,
    #[clap(skip)] pub unique_fields: Vec<usize>,
    #[clap(skip)] pub unique_field_names: Vec<String>,
    // Distribution write-out fields (subset of unique fields) allow names too
    #[arg(short='D', long="write_distros", value_delimiter=',')] pub raw_write_distros: Vec<String>,
    #[clap(skip)] pub write_distros: Vec<usize>,
    #[clap(skip)] pub write_distro_field_names: Vec<String>,
    #[arg(long="write_distros_upper", default_value_t=5)] pub write_distros_upper: usize,
    #[arg(long="write_distros_bottom", default_value_t=2)] pub write_distros_bottom: usize,
    // Aggregates: allow numeric indices OR header names (resolved post header like keys)
    #[arg(short='s', long="sum_values", value_delimiter=',')] pub raw_sum_fields: Vec<String>,
    #[arg(short='a', long="avg_values", value_delimiter=',')] pub raw_avg_fields: Vec<String>,
    #[clap(skip)] pub sum_fields: Vec<usize>,
    #[clap(skip)] pub avg_fields: Vec<usize>,
    #[clap(skip)] pub sum_field_names: Vec<String>,
    #[clap(skip)] pub avg_field_names: Vec<String>,
    // Numeric min/max; string min/max: support names
    #[arg(short='x', long="max_nums", value_delimiter=',')] pub raw_max_num_fields: Vec<String>,
    #[clap(skip)] pub max_num_fields: Vec<usize>,
    #[clap(skip)] pub max_num_field_names: Vec<String>,
    #[arg(short='n', long="min_nums", value_delimiter=',')] pub raw_min_num_fields: Vec<String>,
    #[clap(skip)] pub min_num_fields: Vec<usize>,
    #[clap(skip)] pub min_num_field_names: Vec<String>,
    #[arg(short='X', long="max_strings", value_delimiter=',')] pub raw_max_str_fields: Vec<String>,
    #[clap(skip)] pub max_str_fields: Vec<usize>,
    #[clap(skip)] pub max_str_field_names: Vec<String>,
    #[arg(short='N', long="min_strings", value_delimiter=',')] pub raw_min_str_fields: Vec<String>,
    #[clap(skip)] pub min_str_fields: Vec<usize>,
    #[clap(skip)] pub min_str_field_names: Vec<String>,
    #[arg(short='A', long="field_aliases", value_delimiter=',', value_parser=parse_alias)] pub field_aliases: Option<Vec<(usize,String)>>,
    // Aggregation specs (constructed post name resolution) for unified numeric ops
    #[clap(skip)] pub num_specs: Vec<NumAggSpec>,
    #[clap(skip)] pub str_specs: Vec<StrAggSpec>,
    #[clap(skip)] pub avg_specs: Vec<AvgAggSpec>,
    #[arg(short='r', long="regex")] pub re_str: Vec<String>,
    #[arg(short='p', long="path_re")] pub re_path: Option<String>,
    #[arg(long="re_line_contains")] pub re_line_contains: Option<String>,
    #[arg(short='d', long="input_delimiter", value_parser=parse_escape, default_value=",")] pub delimiter: char,
    #[arg(short='q', long="quote", value_parser=parse_escape)] pub quote: Option<char>,
    #[arg(short='e', long="escape", value_parser=parse_escape)] pub escape: Option<char>,
    #[arg(short='C', long="comment", value_parser=parse_escape)] pub comment: Option<char>,
    #[arg(short='o', long="output_delimiter", default_value=",")] pub od: String,
    #[arg(short='c', long="csv_output")] pub csv_output: bool,
    #[arg(short='v', action=ArgAction::Count)] pub verbose: u8,
    #[arg(long="skip_header")] pub skip_header: bool,
    #[arg(long="no_record_count")] pub no_record_count: bool,
    #[arg(long="empty_string", default_value="")] pub empty: String,
    #[arg(short='t', long="parse_threads", default_value_t = get_default_parse_thread_no() as u64)] pub parse_threads: u64,
    #[arg(short='I', long="io_threads", default_value_t = get_default_io_thread_no() as u64)] pub io_threads: u64,
    #[arg(long="queue_size", default_value_t = get_default_queue_size())] pub thread_qsize: usize,
    #[arg(long="path_qsize", default_value_t = 0)] pub path_qsize: usize,
    #[arg(long="noop_proc")] pub noop_proc: bool,
    #[arg(long="io_block_size", value_parser=parse_human_size, default_value_t = 0)] pub io_block_size: usize,
    #[arg(long="q_block_size", value_parser=parse_human_size, default_value="256K")] pub q_block_size: usize,
    #[arg(short='l', long="file_list")] pub file_list: Option<PathBuf>,
    #[arg(short='i', long="stdin_file_list")] pub stdin_file_list: bool,
    // Accept one or more file values after a single -f like: -f file1 file2
    #[arg(short='f', long="file", num_args=1..)] pub files: Vec<PathBuf>,
    #[arg(short='w', long="walk")] pub walk: Option<String>,
    #[arg(long="stats")] pub stats: bool,
    #[arg(long="no_output")] pub no_output: bool,
    #[arg(long="recycle_io_blocks_disable")] pub recycle_io_blocks_disable: bool,
    #[arg(long="disable_key_sort")] pub disable_key_sort: bool,
    #[arg(long="null_write", default_value="NULL")] pub null: String,
    #[arg(long="mem_stats")] pub mem_stats: bool,
    #[allow(non_snake_case)]
    #[arg(long="ISO-8859")] pub iso_8859: bool,
    #[arg(long="sample_schema")] pub sample_schema: Option<u32>,
    #[arg(long="where_re", value_parser=parse_field_and_regex)] pub where_re: Option<Vec<(usize,Regex_pre2)>>,
    #[arg(long="where_not_re", value_parser=parse_field_and_regex)] pub where_not_re: Option<Vec<(usize,Regex_pre2)>>,
    #[arg(long="head")] pub head: Option<u64>,
    #[arg(long="tail")] pub tail: Option<u64>,
    #[arg(long="count_dsc")] pub count_dsc: bool,
    #[arg(long="count_asc")] pub count_asc: bool,
    #[arg(long="count_ge")] pub count_le: Option<u64>,
    #[arg(long="count_le")] pub count_ge: Option<u64>,
    #[arg(short='E', long="print_examples")] pub print_examples: bool,
}

// ---- Unified aggregation spec models (Step 1: definitions, wiring; logic applied later) ----
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumOpKind { Sum, Min, Max }

#[derive(Debug, Clone, PartialEq)]
pub struct NumAggSpec {
    // Source column index (0-based)
    pub src_index: usize,
    // Destination slot index inside KeySum.nums (0-based)
    pub dest_index: usize,
    pub kind: NumOpKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrOpKind { Min, Max }

#[derive(Debug, Clone, PartialEq)]
pub struct StrAggSpec {
    pub src_index: usize,
    pub dest_index: usize,
    pub kind: StrOpKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvgOpKind { Avg }

#[derive(Debug, Clone, PartialEq)]
pub struct AvgAggSpec {
    pub src_index: usize,
    pub dest_index: usize,
    pub kind: AvgOpKind,
}

fn print_examples() {
    println!(
"Here are a few examples for quick reference

File sources:

cat it.csv | gb -k 1,2 -s 4  # reads from standard-in
find . | gb -i ....          # reads from files piped to standard in
gb -f file1 fil2....         # read from specified files
gb -l <file_list> ....       # reads from files in a list file
gb -w /some/path -p '.*.csv' # read all files under directory that end in .csv
Fields:

gb -f file1 -k 2 -s 3 -a 4 -u 5 --write_distros 5
# reads csv file1 and does a select.. group by 2
# sum field 3;  avg field 4; write value count distro for field 5

ver: {}\n", env!("BUILD_GIT_HASH"));
}

fn from_human_size(s: &str) -> Result<usize> {
    let mut postfix = String::new();
    let mut number = String::new();
    for c in s.chars() {
        if !c.is_ascii_digit() {
            postfix.push(c.to_ascii_lowercase());
        } else {
            number.push(c);
        }
    }
    if number.is_empty() {
        Err(format!("Missing numeric portion in size, found only: \"{}\"", s))?
    }
    if postfix.is_empty() {
        let s: usize = number.parse()?;
        Ok(s)
    } else {
        let num: usize = number.parse()?;
        match postfix.as_str() {
            "k" | "kb" => Ok(num * 1024usize),
            "m" | "mb" => Ok(num * 1024usize * 1024usize),
            "g" | "gb" => Ok(num * 1024usize * 1024usize * 1024usize),
            _ => Err(format!("human size postfix \"{}\" not understood", postfix.as_str()))?
        }
    }
}
fn alias_parser(s: &str) -> Result<(usize, String)> {
    let v = s.split(':').collect::<Vec<_>>();
    if v.len() != 2 {
        Err(format!("alias must come in pairs split by a : you specified: \"{}\"", &s))?;
    }

    let size = match v[0].parse() {
        Err(e) => Err(format!("alias must be number:name - this is not a integer \"{}\" found in {}", v[0], &e))?,
        Ok(s) => s,
    };
    let string = String::from(v[1]);
    Ok((size, string))
}

fn field_and_regex(s: &str) -> Result<(usize, Regex_pre2)> {
    let v = s.split(':').collect::<Vec<_>>();
    if v.len() != 2 {
        Err(format!("regex must come in pairs split by a : you specified: \"{}\"", &s))?;
    }

    let size = match v[0].parse() {
        Err(e) => Err(format!("regex must be number:<Regex> - this is not a integer \"{}\" found in {}", v[0], &e))?,
    Ok(0) => Err("field must 1 or greater".to_string())?,
        Ok(s) => s-1,
    };
    let regex = match Regex_pre2::new(v[1]) {
        Err(e) => Err(format!("regex error for \"{}\" error {}", v[1], &e))?,
        Ok(re) => re,
    };
    Ok((size, regex))
}

fn escape_parser(s: &str) -> Result<char> {
    if let Some(stripped) = s.strip_prefix("\\d") {
        match u8::from_str(stripped) {
            Ok(v) if v <= 127 => Ok(v as char),
            _ => Err(format!("Expect delimiter escape decimal to a be a number between 0 and 127 but got: \"{}\"", stripped))?,
        }
    } else {
        match s {
            "\\t" => Ok('\t'),
            "\\0" => Ok('\0'),
            _ => {
                if s.len() != 1 {
                    Err("Delimiter not understood - must be 1 character OR \\t or \\0 or \\d<dec num>".to_string())?
                }
                Ok(s.chars().next().unwrap())
            }
        }
    }
}

// ---- clap wrapper parsers returning simple String errors (Send + Sync + 'static) ----
fn parse_escape(s: &str) -> std::result::Result<char, String> {
    escape_parser(s).map_err(|e| e.to_string())
}
fn parse_human_size(s: &str) -> std::result::Result<usize, String> {
    from_human_size(s).map_err(|e| e.to_string())
}
fn parse_alias(s: &str) -> std::result::Result<(usize, String), String> {
    alias_parser(s).map_err(|e| e.to_string())
}
fn parse_field_and_regex(s: &str) -> std::result::Result<(usize, Regex_pre2), String> {
    field_and_regex(s).map_err(|e| e.to_string())
}

fn add_n_check(indices: &mut [usize], comment: &str) -> Result<()> {
    let mut last = usize::MAX;
    let mut clone_indices = indices.to_vec();
    clone_indices.sort_unstable();
    for &x in &clone_indices {
        if x == last {
            Err(format!("Field indices must be unique per purpose. Field position {} appears more than once for option {}", x, comment))?;
        }
        last = x;
    }
    // Indices for numeric entries are converted to 0-based immediately when parsed.
    // Do NOT subtract again here (was causing off-by-one / zero errors in tests).
    for x in indices.iter_mut() {
        if *x == usize::MAX { Err(format!("Invalid index overflow for option {}", comment))?; }
    }
    Ok(())
}

pub fn get_cli() -> Result<Arc<CliCfg>> {
    // CliCfg is made immutable for thread saftey - does not need to be
    // changed after a this point.  But, we must using Arc in combination
    // to work around the scope issue.
    // Arc prevents the unneeded copy for cloning when passing to thread.
    // Threads need static scope OR their own copy of a thing
    // The scope inside the new allow the config to be mutable
    // but then put into to th Arc as immutable
    let cfg = Arc::new({
    let mut cfg: CliCfg = CliCfg::parse();
        // Initialize spec vectors empty (populated after potential name resolution)
        cfg.num_specs = Vec::new();
        cfg.str_specs = Vec::new();
        if cfg.print_examples {
            print_examples();
            std::process::exit(1);
        }
        if cfg.re_str.len() > 1 {
            cfg.parse_threads = 1;
            if cfg.verbose >= 1 {
                eprintln!("Override thread number to 1 since you have multiple [{}] REs listed ", cfg.re_str.len());
            }
        }
        fn re_map(v: usize) -> Result<usize> {
            if v == 0 { Err("Field indices must start at base 1".to_string())?; }
            Ok(v - 1)
        }

        // --- key fields custom parsing (support names) ---
        {
            let mut numeric: Vec<usize> = Vec::new();
            let mut names: Vec<String> = Vec::new();
            for tok in &cfg.raw_key_fields {
                let t = tok.trim();
                if t.is_empty() { continue; }
                match t.parse::<usize>() {
                    Ok(v) => {
                        if v == 0 { Err("Field indices must start at base 1".to_string())?; }
                        numeric.push(v - 1);
                    }
                    Err(_) => {
                        names.push(t.to_string());
                    }
                }
            }
            // uniqueness for numeric portion
            if !numeric.is_empty() {
                let mut sorted = numeric.clone();
                sorted.sort_unstable();
                let mut last = usize::MAX;
                for x in &sorted { if *x == last { Err(format!("Field indices must be unique per purpose. Field position {} appears more than once for option -k", x+1))?; } last = *x; }
            }
            // early checks for name usage
            if !names.is_empty() {
                if cfg.re_str.len() > 0 {
                    Err("Header name selectors (-k) are not supported in regex mode yet; use numeric indices.".to_string())?;
                }
                if !cfg.skip_header {
                    Err("Name-based key selectors require --skip_header so the first line is treated as the header.".to_string())?;
                }
            }
            // initialize fields
            cfg.key_fields_idx = RwLock::new(numeric);
            cfg.key_field_names = names.clone();
            cfg.key_fields_resolved = AtomicBool::new(names.is_empty());
            cfg.key_index_names = RwLock::new(HashMap::new());
        }
        // Common helper for parsing mixed numeric/name lists for all aggregate-like options
        {
            let parse_named = |raw: &Vec<String>, dest_idx: &mut Vec<usize>, dest_names: &mut Vec<String>, opt: &str| -> Result<()> {
                for tok in raw {
                    let t = tok.trim();
                    if t.is_empty() { continue; }
                    match t.parse::<usize>() {
                        Ok(v) => {
                            if v == 0 { Err(format!("Field indices must start at base 1 for option {}", opt))?; }
                            dest_idx.push(v-1);
                        }
                        Err(_) => dest_names.push(t.to_string()),
                    }
                }
                // uniqueness on numeric
                if !dest_idx.is_empty() {
                    let mut sorted = dest_idx.clone();
                    sorted.sort_unstable();
                    let mut last = usize::MAX;
                    for x in &sorted { if *x == last { Err(format!("Field indices must be unique for option {} at position {}", opt, x+1))?; } last = *x; }
                }
                Ok(())
            };
            // --- sum & avg fields parsing (support names) ---
            parse_named(&cfg.raw_sum_fields, &mut cfg.sum_fields, &mut cfg.sum_field_names, "-s")?;
            parse_named(&cfg.raw_avg_fields, &mut cfg.avg_fields, &mut cfg.avg_field_names, "-a")?;
            // --- unique / distros ---
            parse_named(&cfg.raw_unique_fields, &mut cfg.unique_fields, &mut cfg.unique_field_names, "-u")?;
            parse_named(&cfg.raw_write_distros, &mut cfg.write_distros, &mut cfg.write_distro_field_names, "-D")?;
            // --- min/max numeric/string ---
            parse_named(&cfg.raw_min_num_fields, &mut cfg.min_num_fields, &mut cfg.min_num_field_names, "-n")?;
            parse_named(&cfg.raw_max_num_fields, &mut cfg.max_num_fields, &mut cfg.max_num_field_names, "-x")?;
            parse_named(&cfg.raw_min_str_fields, &mut cfg.min_str_fields, &mut cfg.min_str_field_names, "-N")?;
            parse_named(&cfg.raw_max_str_fields, &mut cfg.max_str_fields, &mut cfg.max_str_field_names, "-X")?;
        }
        // numeric validation (already 0-based). Name-based portions resolved later.
        add_n_check(&mut cfg.sum_fields, "-s")?;
        add_n_check(&mut cfg.avg_fields, "-a")?;
        add_n_check(&mut cfg.max_num_fields, "-x")?;
        add_n_check(&mut cfg.max_str_fields, "-X")?;
        add_n_check(&mut cfg.min_num_fields, "-n")?;
        add_n_check(&mut cfg.min_str_fields, "-N")?;
        add_n_check(&mut cfg.unique_fields, "-u")?;
        add_n_check(&mut cfg.write_distros, "--write_distros")?;

        if cfg.re_line_contains.is_some() && cfg.re_str.is_empty() {
            Err("re_line_contains requires -r regex option to be used")?;
        }
        for re in &cfg.re_str {
            if let Err(err) = Regex::new(re) { Err(err)? }
        }
        // write_distros subset validation for pure-numeric case (name-based validated after resolution)
        if cfg.write_distro_field_names.is_empty() && cfg.unique_field_names.is_empty() {
            if cfg.write_distros.len() > cfg.unique_fields.len() {
                Err("write_distro fields must be subsets of -u [unique fields]")?;
            }
            for x in &cfg.write_distros {
                if !cfg.unique_fields.contains(x) {
                    Err(format!("write_distro specifies field {} that is not a subset of the unique_keys", x))?;
                }
            }
        }
        if cfg.verbose == 1 {
            eprintln!("CLI options: {:?}", cfg);
        } else if cfg.verbose > 1 {
            eprintln!("CLI options: {:#?}", cfg);
        }
        if cfg.testre.is_none()
            && cfg.key_field_names.is_empty()
            && cfg.key_fields_idx.read().unwrap().is_empty()
            && cfg.sum_fields.is_empty() && cfg.avg_fields.is_empty() && cfg.unique_fields.is_empty()
            && cfg.sum_field_names.is_empty() && cfg.avg_field_names.is_empty()
            && cfg.unique_field_names.is_empty() && cfg.max_num_fields.is_empty() && cfg.min_num_fields.is_empty()
            && cfg.max_str_fields.is_empty() && cfg.min_str_fields.is_empty()
            && cfg.max_num_field_names.is_empty() && cfg.min_num_field_names.is_empty()
            && cfg.max_str_field_names.is_empty() && cfg.min_str_field_names.is_empty() {
            Err("No work to do! - you should specify at least one or more field options or a testre")?;
        }
    if cfg.re_path.is_some() {
            if cfg.files.is_empty() && cfg.file_list.is_none() && !cfg.stdin_file_list && cfg.walk.is_none() {
                Err("Cannot use a re_path setting with STDIN as input.")?;
            }
            let _ = Regex::new(cfg.re_path.as_ref().unwrap())?;
        }
        if cfg.io_block_size != 0
            && cfg.files.is_empty() && cfg.file_list.is_none() && !cfg.stdin_file_list && cfg.walk.is_none() {
            Err("Cannot set io_block_size in stdin mode")?
        }
        if cfg.sample_schema.is_some() {
            cfg.parse_threads = 1;
        }
        // Build initial specs so regex mode (no header resolution) has aggregation definitions.
        cfg.build_specs();
        cfg
    });

    Ok(cfg)
}

impl CliCfg {
    pub fn key_fields(&self) -> RwLockReadGuard<'_, Vec<usize>> { self.key_fields_idx.read().unwrap() }
    // Central validation invoked after name resolution (not during initial numeric parse to avoid false negatives while names pending).
    pub fn validate_config(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        for wd in &self.write_distros { if !self.unique_fields.contains(wd) { Err(format!("write_distro specifies field {} that is not a subset of the unique_keys", wd))?; } }
        // Determine if there is any configured work either numeric indices OR unresolved names waiting on header.
        let no_numeric_work = self.key_fields().is_empty()
            && self.sum_fields.is_empty() && self.avg_fields.is_empty() && self.unique_fields.is_empty()
            && self.min_num_fields.is_empty() && self.max_num_fields.is_empty()
            && self.min_str_fields.is_empty() && self.max_str_fields.is_empty();
        let pending_name_work = !(self.key_field_names.is_empty()
            && self.sum_field_names.is_empty() && self.avg_field_names.is_empty() && self.unique_field_names.is_empty()
            && self.min_num_field_names.is_empty() && self.max_num_field_names.is_empty()
            && self.min_str_field_names.is_empty() && self.max_str_field_names.is_empty());
        if no_numeric_work && !pending_name_work { Err("No work to do!".to_string())?; }
        for a in &self.avg_fields { if self.sum_fields.contains(a) || self.min_num_fields.contains(a) || self.max_num_fields.contains(a) { eprintln!("Warning: avg field {} also present in other numeric aggregate list", a+1); } }
        Ok(())
    }
    // Build unified aggregation spec vectors from already 0-based field index lists.
    // Ordering contract preserved with existing output formatting assumptions:
    //   nums: sums -> mins -> maxs
    //   strs: min_str -> max_str
    pub fn build_specs(&mut self) {
        // numeric specs
        self.num_specs.clear();
        let mut dest_index = 0usize;
        for &src in &self.sum_fields {
            self.num_specs.push(NumAggSpec { src_index: src, dest_index, kind: NumOpKind::Sum });
            dest_index += 1;
        }
        for &src in &self.min_num_fields {
            self.num_specs.push(NumAggSpec { src_index: src, dest_index, kind: NumOpKind::Min });
            dest_index += 1;
        }
        for &src in &self.max_num_fields {
            self.num_specs.push(NumAggSpec { src_index: src, dest_index, kind: NumOpKind::Max });
            dest_index += 1;
        }
        // string specs
        self.str_specs.clear();
        let mut str_dest = 0usize;
        for &src in &self.min_str_fields {
            self.str_specs.push(StrAggSpec { src_index: src, dest_index: str_dest, kind: StrOpKind::Min });
            str_dest += 1;
        }
        for &src in &self.max_str_fields {
            self.str_specs.push(StrAggSpec { src_index: src, dest_index: str_dest, kind: StrOpKind::Max });
            str_dest += 1;
        }
        // average specs (simple pass-through: one dest slot per avg field)
        self.avg_specs.clear();
        for (i, &src) in self.avg_fields.iter().enumerate() {
            self.avg_specs.push(AvgAggSpec { src_index: src, dest_index: i, kind: AvgOpKind::Avg });
        }
    }
    pub fn resolve_key_field_names(&mut self, headers: &csv::StringRecord) -> std::result::Result<(), Box<dyn std::error::Error>> {
        if self.key_fields_resolved.load(AtomicOrdering::Relaxed) { return Ok(()); }
        let mut map = std::collections::HashMap::<&str, usize>::new();
        for (i, h) in headers.iter().enumerate() { map.entry(h).or_insert(i); }
        let mut missing: Vec<String> = Vec::new();
        let mut resolved: Vec<usize> = Vec::new();
    let key_names_snapshot = self.key_field_names.clone();
        for name in &key_names_snapshot {
            match map.get(name.as_str()) {
                Some(idx) => {
                    resolved.push(*idx);
                    // record mapping idx -> name
                    self.key_index_names.write().unwrap().insert(*idx, name.clone());
                },
                None => missing.push(name.clone()),
            }
        }
        if !missing.is_empty() {
            Err(format!("Missing header name(s) for -k: {}", missing.join(", ")))?;
        }
        {
            // Append resolved names to existing numeric indices under lock then drop lock.
            let mut guard = self.key_fields_idx.write().unwrap();
            guard.extend(resolved);
            guard.sort_unstable();
            guard.dedup();
        }
        self.key_fields_resolved.store(true, AtomicOrdering::Relaxed);
        // Resolve aggregate field names after releasing key_fields lock to avoid borrow conflicts.
        let sum_names_snapshot = self.sum_field_names.clone();
        resolve_numeric_name_list(headers, &sum_names_snapshot, &mut self.sum_fields, &self.key_index_names, "-s")?;
        let avg_names_snapshot = self.avg_field_names.clone();
        resolve_numeric_name_list(headers, &avg_names_snapshot, &mut self.avg_fields, &self.key_index_names, "-a")?;
        let uniq_names_snapshot = self.unique_field_names.clone();
        resolve_numeric_name_list(headers, &uniq_names_snapshot, &mut self.unique_fields, &self.key_index_names, "-u")?;
        let wdist_names_snapshot = self.write_distro_field_names.clone();
        resolve_numeric_name_list(headers, &wdist_names_snapshot, &mut self.write_distros, &self.key_index_names, "-D")?;
        let min_num_names_snapshot = self.min_num_field_names.clone();
        resolve_numeric_name_list(headers, &min_num_names_snapshot, &mut self.min_num_fields, &self.key_index_names, "-n")?;
        let max_num_names_snapshot = self.max_num_field_names.clone();
        resolve_numeric_name_list(headers, &max_num_names_snapshot, &mut self.max_num_fields, &self.key_index_names, "-x")?;
        let min_str_names_snapshot = self.min_str_field_names.clone();
        resolve_numeric_name_list(headers, &min_str_names_snapshot, &mut self.min_str_fields, &self.key_index_names, "-N")?;
        let max_str_names_snapshot = self.max_str_field_names.clone();
        resolve_numeric_name_list(headers, &max_str_names_snapshot, &mut self.max_str_fields, &self.key_index_names, "-X")?;
        // After resolving write distros ensure they are subset of unique fields
        for wd in &self.write_distros { if !self.unique_fields.contains(wd) { Err(format!("write_distro specifies field {} that is not a subset of the unique_keys", wd))?; } }

        // Rebuild specs now that any name-based indices have been added.
        self.build_specs();
        // Final validation post resolution
        self.validate_config()?;
        Ok(())
    }
}

fn resolve_numeric_name_list(headers: &csv::StringRecord, names: &Vec<String>, dest: &mut Vec<usize>, name_map: &RwLock<HashMap<usize,String>>, opt: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
    if names.is_empty() { return Ok(()); }
    let mut map = std::collections::HashMap::<&str, usize>::new();
    for (i,h) in headers.iter().enumerate() { map.entry(h).or_insert(i); }
    let mut missing = Vec::<String>::new();
    let mut resolved: Vec<usize> = Vec::new();
    for name in names {
        match map.get(name.as_str()) {
            Some(idx) => {
                resolved.push(*idx);
                name_map.write().unwrap().insert(*idx, name.clone());
            },
            None => missing.push(name.clone()),
        }
    }
    if !missing.is_empty() { Err(format!("Missing header name(s) for {}: {}", opt, missing.join(", "))) ?; }
    dest.extend(resolved);
    dest.sort_unstable();
    dest.dedup();
    Ok(())
}

