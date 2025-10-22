#![allow(dead_code)]
#![allow(unused_imports)]

use atty::Stream;
use crossterm::style::{SetForegroundColor, Color, ResetColor};
use pcre2::bytes::{CaptureLocations as CaptureLocations_pcre2, Captures as Captures_pcre2, Regex as Regex_pre2};
use prettytable::{Cell, format, Row, Table};
use regex::{CaptureLocations, Regex};
use std::alloc::{System, handle_alloc_error};
use std::sync::atomic::AtomicIsize;
use std::{
    cmp::Ordering,
    io::prelude::*,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

mod cli;
mod gen;
mod mem;
mod testre;
mod keysum;

use cli::{get_cli, CliCfg};
use cpu_time::ProcessTime;
use gen::{distro_format, io_thread_slicer, mem_metric_digit, per_file_thread, FileSlice, IoSlicerStatus, MergeStatus, get_reader_writer};

use ignore::WalkBuilder;
use testre::testre;

use mem::{CounterAtomicUsize, CounterTlsToAtomicUsize, CounterUsize, GetAlloc};
use std::ops::Index;
use csv::StringRecord;
use smallvec::SmallVec;
use std::io::BufReader;
use std::cmp::Ordering::{Less, Greater, Equal};
use std::hash::{BuildHasher, Hash};
use lazy_static;
//pub static GLOBAL_TRACKER: System = System; //CounterAtomicUsize = CounterAtomicUsize;
#[cfg(not(target_os = "windows"))]
#[global_allocator]
pub static GLOBAL_TRACKER: jemallocator::Jemalloc = jemallocator::Jemalloc;
//pub static GLOBAL_TRACKER: std::alloc::System = std::alloc::System;
//pub static GLOBAL_TRACKER: CounterTlsToAtomicUsize = CounterTlsToAtomicUsize;

#[cfg(target_os = "windows")]
#[global_allocator]
pub static GLOBAL_TRACKER: CounterTlsToAtomicUsize = CounterTlsToAtomicUsize;

// null char is the BEST field terminator in-the-world but sadly few use it anymore
const KEY_DEL: char = '\0';


// what type of xMap are we using today???
use std::collections::{HashMap, BTreeMap};
use std::fs::File;
use std::ffi::OsString;
use crate::keysum::{sum_maps, store_rec, SchemaSample};
use std::error::Error;
use predicates::str::PredicateStrExt;

//use seahash::SeaHasher;
//use fnv::FnvHashMap;
//use fxhash::FxHashMap;

//type MyMap = FnvHashMap<String, KeySum>;
//type MyMap = FxHashMap<String, KeySum>;
fn create_map() -> MyMap
{
    MyMap::default()//(1000, SeaHasher::default())
}

type MyMap = BTreeMap<String, keysum::KeySum>;



fn main() {
    if let Err(err) = csv() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
    }
}

fn stat_ticker(verbosity: usize, thread_stopper: Arc<AtomicBool>, io_status: &mut Arc<IoSlicerStatus>, 
               merge_status: &Arc<MergeStatus>,
               phase: &Arc<AtomicIsize>,
               send: &crossbeam_channel::Sender<Option<FileSlice>>,
               filesend: &crossbeam_channel::Sender<Option<PathBuf>>) {
    let start_f = Instant::now();
    let startcpu = ProcessTime::now();
   
    use crossterm::{terminal::Clear, terminal::ClearType::CurrentLine, style::{Color, ResetColor, SetForegroundColor}};

    let (sleeptime, return_or_not) = if verbosity > 0 {
        // slower ticket when verbose output is on and newlines
        (1000, "\n".to_string())
    } else {
        // action ticket if not other output on
        (250, format!("{}","\r"))
    };
    loop {
        thread::sleep(Duration::from_millis(sleeptime));
        if thread_stopper.load(Relaxed) {
            break;
        }
        let phasel = phase.load(std::sync::atomic::Ordering::Relaxed);
        if phasel == ProcPhase::READ as isize {
            let total_bytes = io_status.bytes.load(std::sync::atomic::Ordering::Relaxed);
            let file_count = io_status.files.load(std::sync::atomic::Ordering::Relaxed);
            let elapsed = start_f.elapsed();
            let sec: f64 = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
            let rate = (total_bytes as f64 / sec) as usize;
            let elapsedcpu: Duration = startcpu.elapsed();
            let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1_000_000_000.0);
            {
                let curr_file = io_status.curr_file.lock().unwrap();
                
                eprint!("{}{} q: {}/{}/{}  {}  rate: {}/s at  time(sec): {:.3}  cpu(sec): {:.3}  curr: {}  mem: {}{}{}",
                    Clear(CurrentLine),
                    SetForegroundColor(Color::Green),
                    send.len(),
                    file_count,
                    filesend.len(),
                    mem_metric_digit(total_bytes, 4),
                    mem_metric_digit(rate, 4),
                    sec,
                    seccpu,
                    curr_file,
                    mem_metric_digit(GLOBAL_TRACKER.get_alloc(), 4),
                    return_or_not,
                    ResetColor,
                );
            }
        } else if phasel == ProcPhase::MERGE as isize {
            let cur = merge_status.current.load(Relaxed);
            let tot = merge_status.total.load(Relaxed);
            if tot > 0 {
                let per = (cur as f64 / tot as f64)*100.0;
                
                eprint!("{}{}Merging map entries {} of {}  {:.2}% complete  mem: {}{}{}", 
                    Clear(CurrentLine),
                    SetForegroundColor(Color::Green),
                    cur, tot, per, mem_metric_digit(GLOBAL_TRACKER.get_alloc(), 4),
                    return_or_not, ResetColor);
            } else {
                eprint!("{}{}Merging map ....{}{}", 
                    Clear(CurrentLine),
                    SetForegroundColor(Color::Green),
                    return_or_not, ResetColor);
            }

        } else if phasel == ProcPhase::SORT as isize {
            eprint!("{}{}Sorting... {}{}", Clear(CurrentLine), SetForegroundColor(Color::Green), return_or_not, ResetColor);
        } 
    }
    eprint!("\r{}", Clear(CurrentLine));
}

enum ProcPhase{
    SETUP=0isize,
    READ=1isize,
    SORT=2isize,
    MERGE=3isize,
    OUTPUT=4isize,
}

fn type_name_of<T: core::any::Any>(_t: &T) -> &str {
    std::any::type_name::<T>()
}

fn csv() -> Result<(), Box<dyn std::error::Error>> {
    
    let phase = Arc::new(AtomicIsize::new(ProcPhase::SETUP as isize));

    let start_f = Instant::now();
    let startcpu = ProcessTime::now();


    let cfg = get_cli()?;

    if cfg.verbose >= 1 {
        eprintln!("Global allocator / tracker: {}", type_name_of(&GLOBAL_TRACKER));
    }

    if cfg.verbose >= 1 && pcre2::is_jit_available() {
        eprintln!("pcre2 JIT is available");
    }

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_fieldskipped = 0usize;
    let mut total_lines_skipped = 0usize;
    let mut total_blocks = 0usize;
    let mut total_bytes = 0usize;

    if cfg.testre.is_some() {
        testre(&cfg)?;
        return Ok(());
    }
    if cfg.verbose >= 1 {
        eprintln!("map type: {}", type_name_of(&MyMap::default()));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////

    let mut worker_handlers = vec![];
    let (send_fileslice, recv_fileslice): (crossbeam_channel::Sender<Option<FileSlice>>, crossbeam_channel::Receiver<Option<FileSlice>>) =
        crossbeam_channel::bounded(cfg.thread_qsize);

    let do_stdin = cfg.files.is_empty() && cfg.walk.is_none();
    let mut io_status = Arc::new(IoSlicerStatus::new());
    let mut merge_status = Arc::new(MergeStatus::new());

    let (send_blocks, recv_blocks): (crossbeam_channel::Sender<Vec<u8>>, crossbeam_channel::Receiver<Vec<u8>>) = crossbeam_channel::unbounded();
    if !cfg.recycle_io_blocks_disable {
        for _ in 0..cfg.thread_qsize {
            let ablock = vec![0u8; cfg.q_block_size];
            send_blocks.send(ablock)?;
        }
    }

    // start processing threads
    for no_threads in 0..cfg.parse_threads {
        let cfg = cfg.clone();
        let clone_recv_fileslice = recv_fileslice.clone();
        let clone_senc_blocks = send_blocks.clone();
        let h = match cfg.re_str.len() {
            0 => thread::Builder::new()
                .name(format!("worker_csv{}", no_threads))
                .spawn(move || worker_csv(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
            1 => thread::Builder::new()
                .name(format!("worker_re{}", no_threads))
                .spawn(move || worker_re(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
            _ => thread::Builder::new()
                .name(format!("wr_mul_re{}", no_threads))
                .spawn(move || worker_multi_re(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
        };
        worker_handlers.push(h);
    }

    // SETUP IO

    // IO slicer threads
    let mut io_handler = vec![];
    let (send_pathbuff, recv_pathbuff): (crossbeam_channel::Sender<Option<PathBuf>>, crossbeam_channel::Receiver<Option<PathBuf>>) =
        if cfg.path_qsize > 0 {
            crossbeam_channel::bounded(cfg.thread_qsize)
        } else {
            crossbeam_channel::unbounded()
        };

    for no_threads in 0..cfg.io_threads {
        let clone_cfg = cfg.clone();
        let clone_recv_pathbuff = recv_pathbuff.clone();
        let clone_send_fileslice = send_fileslice.clone();
        let clone_recv_blocks = recv_blocks.clone();
        let io_status_cloned = io_status.clone();
        let path_re: Option<Regex_pre2> = clone_cfg.re_path.as_ref().and_then(|x| Some(Regex_pre2::new(&x).unwrap()));
        let h = thread::Builder::new()
            .name(format!("worker_io{}", no_threads))
            .spawn(move || {
                per_file_thread(
                    clone_cfg.recycle_io_blocks_disable,
                    &clone_recv_blocks,
                    &clone_recv_pathbuff,
                    &clone_send_fileslice,
                    clone_cfg.io_block_size,
                    clone_cfg.q_block_size,
                    clone_cfg.verbose,
                    io_status_cloned,
                    &path_re,
                )
            })
            .unwrap();
        io_handler.push(h);
    }

    //
    // TICKER - setup statistics ticker thread in case we get bored waiting on things
    //
    let stopper = Arc::new(AtomicBool::new(false));
    let thread_stopper = stopper.clone();
    let do_ticker = atty::is(Stream::Stderr) && /* !do_stdin && */ !(cfg.verbose >= 3);
    let _ticker = {
        if do_ticker {
            let mut io_status_cloned = io_status.clone();
            let clone_send = send_fileslice.clone();
            let clone_pathsend = send_pathbuff.clone();
            let phase_c = phase.clone();
            let merge_status = merge_status.clone();
            let verbose = cfg.verbose;
            Some(thread::spawn(move || stat_ticker(verbose, thread_stopper, &mut io_status_cloned, &merge_status, &phase_c, &clone_send, &clone_pathsend)))
        } else {
            None
        }
    };

    phase.store(ProcPhase::READ as isize, Relaxed);
    //
    // FILE - setup feed by the source of data
    //
    if cfg.stdin_file_list {
        if cfg.verbose >= 1 {
            eprintln!("reading files to process from stdin");
        }
        for fileline in std::io::stdin().lock().lines() {
            let line = fileline.expect("Unable to read line from stdin");
            let pathbuf = PathBuf::from(line);
            send_pathbuff
                .send(Some(pathbuf.clone()))
                .unwrap_or_else(|_| eprintln!("Unable to send path: {} to path queue", pathbuf.display()));
        }
    } else if let Some(path) = &cfg.file_list {
        if cfg.verbose >= 1 {
            eprintln!("reading files to process from file: {}", path.display());
        }
        let f = File::open(path).expect(format!("Unable to open file list file: {}", path.display()).as_str());
        let f = BufReader::new(f);
        for fileline in f.lines() {
            let line = fileline.expect(format!("Unable to read line from {}", path.display()).as_str());
            let pathbuf = PathBuf::from(line);
            send_pathbuff
                .send(Some(pathbuf.clone()))
                .unwrap_or_else(|_| eprintln!("Unable to send path: {} to path queue", pathbuf.display()));
        }
    } else if do_stdin {
        eprintln!("{}{}{}",SetForegroundColor(Color::Blue), "<<< reading from stdin", ResetColor);
        let stdin = std::io::stdin();
        let mut handle = stdin; // .lock();
        let empty_vec: Vec<String> = Vec::new();

        let (blocks, bytes) = io_thread_slicer(
            &recv_blocks,
            &"STDIO".to_string(),
            &empty_vec,
            cfg.q_block_size,
            cfg.recycle_io_blocks_disable,
            cfg.verbose,
            &mut handle,
            &mut io_status,
            &send_fileslice,
        )?;
        total_bytes += bytes;
        total_blocks += blocks;
    } else if !cfg.files.is_empty() {
        let filelist = &cfg.files;
        for path in filelist {
            send_pathbuff
                .send(Some(path.clone()))
                .unwrap_or_else(|_| eprintln!("Unable to send path: {} to path queue", path.display()));
        }
    } else if cfg.walk.is_some() {
        let root = PathBuf::from(cfg.walk.as_ref().unwrap());
        for result in WalkBuilder::new(root).hidden(false).build() {
            match result {
                Ok(p) =>
                    send_pathbuff
                        .send(Some(p.clone().into_path()))
                        .expect(&format!("Unable to send path: {} to path queue", p.into_path().display())),
                Err(err) => eprintln!("Error: {}", err),
            }
        }
    } else {
        eprintln!("NOT sure where files are coming from...");
        std::process::exit(1);
    }

    if cfg.verbose > 1 {
        eprintln!("sending stops to thread");
    }

    for _i in 0..cfg.io_threads {
        send_pathbuff.send(None)?;
    }
    if cfg.verbose > 1 {
        eprintln!("joining io handles");
    }
    for h in io_handler {
        let (blocks, bytes) = h.join().unwrap();
        total_bytes += bytes;
        total_blocks += blocks;
    }

    // proc workers on slices can ONLY be told to stop AFTER the IO threads are done
    for _i in 0..cfg.parse_threads {
        send_fileslice.send(None)?;
    
    }
    
    phase.store(ProcPhase::MERGE as isize, Relaxed);

    // merge the data from the workers
    let mut all_the_maps = vec![];
    for h in worker_handlers {
        let (map, linecount, fieldcount, fieldskipped, lines_skipped) = h.join().unwrap();
        total_rowcount += linecount;
        total_fieldcount += fieldcount;
        total_fieldskipped += fieldskipped;
        total_lines_skipped += lines_skipped;
        all_the_maps.push(map);
    }
    let main_map = if !cfg.no_output {
        sum_maps(&mut all_the_maps, cfg.verbose, &cfg, &mut merge_status)
    } else {
        create_map()
    };

    if cfg.verbose > 0 { eprintln!("dropping other maps"); }
    all_the_maps.clear();

    stopper.swap(true, Relaxed);

    // OUTPUT
    // write the data structure
    //

    // restore indexes to users input - really just makes testing slightly easier
    fn re_mod_idx<T>(_cfg: &CliCfg, v: T) -> T
        where
            T: std::ops::Sub<Output=T> + std::ops::Add<Output=T> + From<usize>,
    {
        v + 1.into()
    }
    if do_ticker {
        eprintln!();
    } // write extra line at the end of stderr in case the ticker munges things

    let startout = Instant::now();

    let mut thekeys: Vec<_> = main_map.keys().clone().collect();

    // may add other comparisons but for now - if ON just this one
    let excel_sort_cmp = {
        // partially taken from:
        // https://github.com/DanielKeep/rust-scan-rules/blob/master/src/input.rs#L610-L620
        // but enhanced as a generic Ordering
        fn str_cmp_ignore_case(a: &str, b: &str) -> Ordering {
            let mut acs = a.chars().flat_map(char::to_lowercase);
            let mut bcs = b.chars().flat_map(char::to_lowercase);
            loop {
                match (acs.next(), bcs.next()) {
                    (Some(a), Some(b)) => {
                        let x = a.cmp(&b);
                        if x == Equal { continue; } else { return x; }
                    }
                    (None, None) => return Equal,
                    (Some(_), None) => return Greater, // "aaa" will appear before "aaaz"
                    (None, Some(_)) => return Less,
                }
            }
        }
        |l_k: &&str, r_k: &&str| -> Ordering {
            for (l, r) in l_k.split(KEY_DEL).zip(r_k.split(KEY_DEL)) {
                let res: Ordering = {
                    // compare as numbers if you can
                    let lf = l.parse::<f64>();
                    let rf = r.parse::<f64>();
                    match (lf, rf) {
                        (Ok(lv), Ok(rv)) => lv.partial_cmp(&rv).unwrap_or(Equal),
                        // fall back to string comparison
                        (Err(_), Err(_)) => str_cmp_ignore_case(&l, &r), // l.cmp(r),
                        // here Err means not a number while Ok means a number
                        // number is less than string
                        (Ok(_), Err(_)) => Less,
                        // string is greater than number
                        (Err(_), Ok(_)) => Greater,
                    }
                };
                if res != Equal {
                    return res;
                } // else keep going if this level is equal
            }
            Equal
        }
    };

    let count_cmp_asc = {
        |l_k: &&str, r_k: &&str| -> Ordering {
            let ks_l = main_map.get(*l_k).unwrap();
            let ks_r = main_map.get(*r_k).unwrap();
            ks_l.count.cmp(&ks_r.count)
        }
    };

    let count_cmp_dsc = {
        |l_k: &&str, r_k: &&str| -> Ordering {
            let ks_l = main_map.get(*l_k).unwrap();
            let ks_r = main_map.get(*r_k).unwrap();
            ks_r.count.cmp(&ks_l.count)
        }
    };


    if !cfg.disable_key_sort {
        
        phase.store(ProcPhase::SORT as isize, Relaxed);

        let cpu_keysort_s = ProcessTime::now();
        if cfg.count_dsc {
            thekeys.sort_unstable_by(|l, r| { count_cmp_dsc(&l.as_str(), &r.as_str()) });
        } else if cfg.count_asc {
            thekeys.sort_unstable_by(|l, r| { count_cmp_asc(&l.as_str(), &r.as_str()) });
        } else {
            thekeys.sort_unstable_by(|l, r| { excel_sort_cmp(&l.as_str(), &r.as_str()) });
        }
        if cfg.verbose > 0 {
            let elapsedcpu: Duration = cpu_keysort_s.elapsed();
            let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1_000_000_000.0);
            eprintln!("sort cpu time {:.3}s for {} entries", seccpu, thekeys.len());
        }
    }

    phase.store(ProcPhase::OUTPUT as isize, Relaxed);
    let alias = |field: usize, del: char| -> String {
        match &cfg.field_aliases {
            None => return field.to_string(),
            Some(list) => {
                for (index, s) in list.iter() {
                    if *index == field {
                        return format!("{}{}{}", field, del, s.clone());
                    }
                }
            }
        }
        return field.to_string();
    };

    let alias_c = |field: usize| -> String {
        return alias(field, ':');
    };

    let alias_m = |field: usize| -> String {
        return alias(field, '\n');
    };

    if !cfg.no_output {
        let mut line_count = 0u64;
        let total_lines_expected = thekeys.len() as u64;
        if !cfg.csv_output {
            let _print_skipped = false; // for the stuff after head and before tail
            let mut celltable = Table::new();
            celltable.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            {
                let mut vcell = vec![];
                if cfg.key_fields.len() > 0 {
                    for x in &cfg.key_fields {
                        vcell.push(Cell::new(&format!("k:{}", alias_m(re_mod_idx(&cfg, *x)))));
                    }
                } else {
                    vcell.push(Cell::new("k:-"));
                }
                if !cfg.no_record_count {
                    vcell.push(Cell::new("count"));
                }
                for x in &cfg.sum_fields {
                    vcell.push(Cell::new(&format!("sum:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.min_num_fields {
                    vcell.push(Cell::new(&format!("min:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.max_num_fields {
                    vcell.push(Cell::new(&format!("max:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.avg_fields {
                    vcell.push(Cell::new(&format!("avg:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.min_str_fields {
                    vcell.push(Cell::new(&format!("minstr:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.max_str_fields {
                    vcell.push(Cell::new(&format!("maxstr:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                for x in &cfg.unique_fields {
                    vcell.push(Cell::new(&format!("cnt_uniq:{}", alias_m(re_mod_idx(&cfg, *x)))));
                }
                let row = Row::new(vcell);
                celltable.set_titles(row);
            }

            // let check_ht: &Fn(u64) -> bool = match (cfg.head.clone(), cfg.tail.clone()) {
            //     (None, None) =>                                 &|l| -> bool { if l > 0 {false } else { true}},
            //     (Some(head), None) =>                 &|l| -> bool  { if head < l { false } else { true } },
            //     (None, Some( tail)) =>                 &|l| -> bool  { if tail < (total_lines_expected-l) { false } else { true }},
            //     (Some( head), Some( tail)) => &|l| -> bool { { true }},
            // };


            'KEYS_CELL_LOOP: for ff in thekeys.iter() {
                let cc = main_map.get(*ff).unwrap();

                if let Some(count_ge) = cfg.count_ge {
                    if cc.count > count_ge { continue 'KEYS_CELL_LOOP; } // to skip
                }
                if let Some(count_le) = cfg.count_le {
                    if cc.count < count_le { continue 'KEYS_CELL_LOOP; } // to skip
                }

                line_count += 1;

                let print_this_line: bool = match (cfg.head, cfg.tail) {
                    (None, None) => { true }
                    (Some(head), None) =>
                        if head >= line_count { true } else { false },
                    (None, Some(tail)) => if tail > (total_lines_expected - line_count) { true } else { false }
                    (Some(head), Some(tail)) => {
                        if head >= line_count || tail > (total_lines_expected - line_count) { true } else { false }
                    }
                };
                if !print_this_line { continue 'KEYS_CELL_LOOP; }

                let mut vcell = vec![];
                ff.split(KEY_DEL).for_each(|x| {
                    if x.len() == 0 {
                        vcell.push(Cell::new(&cfg.empty));
                    } else {
                        vcell.push(Cell::new(&x));
                    }
                });


                if !cfg.no_record_count {
                    vcell.push(Cell::new(&format!("{}", cc.count)));
                }
                for x in &cc.nums {
                    match x {
                        Some(x) => vcell.push(Cell::new(&format!("{}", x))),
                        None => vcell.push(Cell::new(&format!("{}", &cfg.null))),
                    };
                }
                for x in &cc.avgs {
                    if x.1 == 0 {
                        vcell.push(Cell::new("unknown"));
                    } else {
                        vcell.push(Cell::new(&format!("{}", (x.0 / (x.1 as f64)))));
                    }
                }
                for x in &cc.strs {
                    match x {
                        Some(x) => vcell.push(Cell::new(&format!("{}", x))),
                        None => vcell.push(Cell::new(&format!("{}", &cfg.null))),
                    };
                }

                for i in 0usize..cc.distinct.len() {
                    if cfg.write_distros.contains(&cfg.unique_fields[i]) {
                        vcell.push(Cell::new(&format!("{}", distro_format(&cc.distinct[i], cfg.write_distros_upper, cfg.write_distros_bottom))));
                    } else {
                        vcell.push(Cell::new(&format!("{}", &cc.distinct[i].len())));
                    }
                }
                let row = Row::new(vcell);
                celltable.add_row(row);
            }
            let stdout = std::io::stdout();
            let _writerlock = stdout.lock();
            let (_reader, mut writer) = get_reader_writer();

            celltable.print(&mut writer)?;
        } else {
            let stdout = std::io::stdout();
            let _writerlock = stdout.lock();
            let (_reader, mut writer) = get_reader_writer();

            let mut line_out = String::with_capacity(180);
            {
                if cfg.key_fields.len() > 0 {
                    for x in &cfg.key_fields {
                        line_out.push_str(&format!("k:{}{}", alias_c(re_mod_idx(&cfg, *x)), &cfg.od));
                    }
                    line_out.truncate(line_out.len() - 1);
                } else {
                    line_out.push_str(&format!("{}k:-", &cfg.od));
                }
                if !cfg.no_record_count {
                    line_out.push_str(&format!("{}count", &cfg.od));
                }
                for x in &cfg.sum_fields {
                    line_out.push_str(&format!("{}sum:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.min_num_fields {
                    line_out.push_str(&format!("{}min:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.max_num_fields {
                    line_out.push_str(&format!("{}max:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.avg_fields {
                    line_out.push_str(&format!("{}avg:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.min_str_fields {
                    line_out.push_str(&format!("{}minstr:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.max_str_fields {
                    line_out.push_str(&format!("{}maxstr:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                for x in &cfg.unique_fields {
                    line_out.push_str(&format!("{}cnt_uniq:{}", &cfg.od, alias_c(re_mod_idx(&cfg, *x))));
                }
                line_out.push('\n');
                writer.write_all(&line_out.as_bytes())?;
            }
            'KEYS_CSV_LOOP: for ff in thekeys.iter() {
                let cc = main_map.get(*ff).unwrap();
                if let Some(count_ge) = cfg.count_ge {
                    if cc.count > count_ge { continue 'KEYS_CSV_LOOP; } // to skip
                }
                if let Some(count_le) = cfg.count_le {
                    if cc.count < count_le { continue 'KEYS_CSV_LOOP; } // to skip
                }

                line_count += 1;
                let print_this_line: bool = match (cfg.head, cfg.tail) {
                    (None, None) => { true }
                    (Some(head), None) =>
                        if head >= line_count { true } else { false },
                    (None, Some(tail)) => if tail > (total_lines_expected - line_count) { true } else { false }
                    (Some(head), Some(tail)) => {
                        if head >= line_count || tail > (total_lines_expected - line_count) { true } else { false }
                    }
                };
                if !print_this_line { continue 'KEYS_CSV_LOOP; }

                line_out.clear();
                let keyv: Vec<&str> = ff.split(KEY_DEL).collect();
                for i in 0..keyv.len() {
                    if keyv[i].len() == 0 {
                        line_out.push_str(&cfg.empty);
                    } else {
                        line_out.push_str(&keyv[i]);
                    }
                    if i < keyv.len() - 1 { line_out.push_str(&cfg.od); }
                }
                if !cfg.no_record_count {
                    line_out.push_str(&format!("{}{}", &cfg.od, cc.count));
                }
                for x in &cc.nums {
                    match x {
                        Some(x) => line_out.push_str(&format!("{}{}", &cfg.od, x)),
                        None => line_out.push_str(&format!("{}{}", &cfg.od, &cfg.null)),
                    };
                }
                for x in &cc.avgs {
                    line_out.push_str(&format!("{}{}", &cfg.od, x.0 / (x.1 as f64)));
                }
                for x in &cc.strs {
                    line_out.push_str(&cfg.od);
                    match x {
                        Some(x) => line_out.push_str(&format!("{}", x)),
                        None => line_out.push_str(&cfg.null),
                    };
                }
                for i in 0usize..cc.distinct.len() {
                    if cfg.write_distros.contains(&cfg.unique_fields[i]) {
                        line_out.push_str(&cfg.od);
                        line_out.push_str(&distro_format(&cc.distinct[i], cfg.write_distros_upper, cfg.write_distros_bottom));
                    } else {
                        line_out.push_str(&format!("{}{}", &cfg.od, cc.distinct[i].len()));
                    }
                }
                line_out.push('\n');
                writer.write_all(&line_out.as_bytes())?;
            }
        }
    }
    let endout = Instant::now();
    let outdur = endout - startout;
    if cfg.verbose > 0 {
        eprintln!("output composition/write time: {:.3}", outdur.as_millis() as f64 / 1000.0);
    }
    if cfg.verbose >= 1 || cfg.stats {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1_000_000_000.0);
        let rate: f64 = (total_bytes as f64) as f64 / sec;
        let total_files = io_status.files.load(std::sync::atomic::Ordering::Relaxed);
        let elapsedcpu: Duration = startcpu.elapsed();
        let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1_000_000_000.0);
        eprintln!(
            "files: {}  read: {}  blocks: {}  rows: {}  fields: {}  rate: {}/s  time: {:.3}  cpu: {:.3}  mem: {}",
            total_files,
            mem_metric_digit(total_bytes, 5),
            total_blocks,
            total_rowcount,
            total_fieldcount,
            mem_metric_digit(rate as usize, 5),
            sec,
            seccpu,
            mem_metric_digit(GLOBAL_TRACKER.get_alloc(), 4)
        );
    }
    if total_fieldskipped > 0 {
        eprintln!("Some fields were skipped {}, as they could not be parsed as numeric.", total_fieldskipped);
    }
    if total_lines_skipped > 0 {
        eprintln!("Note {} records/lines were filtered", total_lines_skipped);
    }

    Ok(())
}

fn worker_re(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize, usize, usize) {
    // return lines / fields
    match _worker_re(&send_blocks, cfg, recv) {
        Ok((map, lines, fields, fieldskipped, lines_skipped)) => (map, lines, fields, fieldskipped, lines_skipped),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!("{}", err_msg);
        }
    }
}

#[derive(Debug)]
struct CapWrap<'t> {
    pub cl: &'t CaptureLocations_pcre2,
    pub text: &'t str,
}
//T: std::ops::Index<usize> + std::fmt::Debug,
//<T as std::ops::Index<usize>>::Output: AsRef<str>,

impl<'t> Index<usize> for CapWrap<'t> {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        self.cl.get(i).map(|m| &self.text[m.0..m.1]).unwrap_or_else(|| panic!("no group at index '{}'", i))
    }
}

impl<'t> CapWrap<'t> {
    fn len(&self) -> usize {
        self.cl.len()
    }
}

fn _worker_re(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = create_map();

    let re_str = &cfg.re_str[0];
    if cfg.verbose > 2 {
        eprintln!("Start of {}", thread::current().name().unwrap());
    }
    let re = match Regex_pre2::new(re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", re_str, err),
        Ok(r) => r,
    };
    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut fieldskipped = 0;
    let mut lines_skipped = 0;
    let mut rowcount = 0;
    let mut _skipped = 0;
    let mut cl = re.capture_locations();

    let mut latin_str_buff = String::with_capacity(1024);
    let mut err_line_fix = String::with_capacity(256);

    let mut sch = if cfg.sample_schema.is_some() {
        Some(SchemaSample::new(cfg.sample_schema.unwrap()))
    } else {
        None
    };

    'BLOCK_LOOP: loop {
        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
                break;
            }
        };
        use std::io::BufRead;

        if !cfg.noop_proc {
            let slice = if cfg.ISO_8859 {
                latin_str_buff.clear();
                fc.block[0..fc.len].iter().map(|&c| c as char).for_each(|c| latin_str_buff.push(c));
                latin_str_buff.as_bytes()
            } else {
                &fc.block[0..fc.len]
            };

            'LINE_LOOP: for (_inner_count, line) in slice.split(|c| *c == b'\n').enumerate() {
                let line = match std::str::from_utf8(&line) {
                    Err(e) => {
                        ascii_line_fixup_full(line, &mut err_line_fix);
                        eprintln!("skipping line due with UTF8 decoding error (try ISO-8859 option?): {}\n\tline: {}", e, &err_line_fix);
                        continue 'LINE_LOOP;
                    }
                    Ok(line) => line
                };

                if let Some(ref line_contains) = cfg.re_line_contains {
                    if !line.contains(line_contains) {
                        if cfg.verbose > 3 {
                            println!("DBG: re_line_contains skip line: {}", line);
                        }
                        continue;
                    }
                }

                if let Some(_record) = re.captures_read(&mut cl, line.as_bytes())? {
                    let mut v = SmallVec::<[&str; 16]>::new();
                    //let mut v: Vec<&str> = Vec::with_capacity(cl.len()+fc.sub_grps.len());
                    for x in fc.sub_grps.iter() {
                        v.push(x);
                    }
                    for i in 1..cl.len() {
                        match cl.get(i) {
                            Some(t) => v.push(&line[t.0..t.1]),
                            None => v.push(""),
                        }
                    }
                    if cfg.verbose > 2 { eprintln!("DBG RE WITH FILE PASTS VEC: {:?}", v); }
                    match sch {
                        Some(ref mut sch) => {
                            sch.schema_rec(&v, v.len());
                            if sch.done() {
                                break 'BLOCK_LOOP;
                            }
                        }
                        _ => {
                            let (fc, fs, ls) = store_rec(&mut buff, "", &v, v.len(), &mut map, &cfg, &mut rowcount);
                            fieldcount += fc;
                            fieldskipped += fs;
                            lines_skipped += ls;
                            // cnt_rec += 1;
                        }
                    }
                } else {
                    _skipped += 1;
                }
            }
        }
        if !cfg.recycle_io_blocks_disable {
            send_blocks.send(fc.block)?;
        }
    }
    if let Some(mut sch) = sch { sch.print_schema(&cfg); }
    Ok((map, rowcount, fieldcount, fieldskipped, lines_skipped))
}

fn worker_csv(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize, usize, usize) {
    match _worker_csv(&send_blocks, cfg, recv) {
        Ok((map, lines, fields, fieldskipped, lines_skipped)) => (map, lines, fields, fieldskipped, lines_skipped),
        Err(e) => {
            let err_msg = format!("Unable to process inner block - likely compressed or not UTF8 text: {}", e);
            panic!("{}", err_msg);
        }
    }
}

fn _worker_csv(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = create_map();

    let mut csv_builder = create_csv_builder(&cfg);

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut fieldskipped = 0;
    let mut lines_skipped = 0;
    let mut rowcount = 0;

    let mut latin_str_buff = String::with_capacity(246);
    let mut err_line_fix = String::with_capacity(256);

    let mut sch = if cfg.sample_schema.is_some() {
        Some(SchemaSample::new(cfg.sample_schema.unwrap()))
    } else {
        None
    };

    'BLOCK_LOOP: loop {
        let fc = match recv.recv().unwrap() {
            Some(fc) => fc,
            None => {
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
                break;
            }
        };
        let add_subs = fc.sub_grps.len() > 0;

        if !cfg.noop_proc {
            let lbuff = if cfg.ISO_8859 {
                latin_str_buff.clear();
                fc.block[0..fc.len].iter().map(|&c| c as char).for_each(|c| latin_str_buff.push(c));
                latin_str_buff.as_bytes()
            } else {
                &fc.block[0..fc.len]
            };

            let mut recrdr = csv_builder.from_reader(lbuff);
            if add_subs {
                for record in recrdr.records() {
                    match record {
                        Ok(record) => {
                            let mut v = SmallVec::<[&str; 16]>::new();
                            fc.sub_grps.iter().map(|x| x.as_str()).chain(record.iter())
                                .for_each(|x| v.push(x)); //
                            if cfg.verbose > 2 { eprintln!("DBG WITH FILE PASTS VEC: {:?}", v); }
                            match sch {
                                Some(ref mut sch) => {
                                    sch.schema_rec(&v, v.len());
                                    if sch.done() {
                                        break 'BLOCK_LOOP;
                                    }
                                }
                                _ => {
                                    let (fc, fs, ls) = store_rec(&mut buff, "", &record, record.len(), &mut map, &cfg, &mut rowcount);
                                    fieldcount += fc;
                                    fieldskipped += fs;
                                    lines_skipped += ls;
                                }
                            };
                            v.clear();
                        }
                        Err(e) => {
                            match e.kind() {
                                csv::ErrorKind::Utf8 { err: _, pos } => {
                                    if let Some(pos) = pos {
                                        ascii_line_fixup(&fc.block, pos.byte() as usize, &mut err_line_fix);
                                        eprintln!("skipping line due to bad UTF8 codes in file (try ISO-8859 option)\n\tline:{}", &err_line_fix);
                                    }
                                }
                                _ => eprintln!("skipping line due to err: {:?}", e),
                            }
                        }
                    }
                }
            } else {
                let mut cnt_rec = 0usize;
                for record in recrdr.records() {
                    match record {
                        Ok(record) => {
                            match sch {
                                Some(ref mut sch) => {
                                    sch.schema_rec(&record, record.len());
                                    if sch.done() {
                                        break 'BLOCK_LOOP;
                                    }
                                }
                                _ => {
                                    let (fc, fs, ls) = store_rec(&mut buff, "", &record, record.len(), &mut map, &cfg, &mut rowcount);
                                    fieldcount += fc;
                                    fieldskipped += fs;
                                    lines_skipped += ls;
                                    cnt_rec += 1;
                                }
                            };
                        }
                        Err(e) => {
                            match e.kind() {
                                csv::ErrorKind::Utf8 { err: _, pos } => {
                                    if let Some(pos) = pos {
                                        ascii_line_fixup(&fc.block, pos.byte() as usize, &mut err_line_fix);
                                        eprintln!("skipping line due to bad UTF8 codes in file (try ISO-8859 option)\n\tline:{}", &err_line_fix);
                                    }
                                }
                                _ => eprintln!("skipping line due to err: {:?}", e),
                            }
                        }
                    }
                }
            }
        }
        if !cfg.recycle_io_blocks_disable {
            send_blocks.send(fc.block)?;
        }
    }
    if let Some(mut sch) = sch { sch.print_schema(&cfg); }

    Ok((map, rowcount, fieldcount, fieldskipped, lines_skipped))
}

fn worker_multi_re(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize, usize, usize) {
    // return lines / fields
    match _worker_multi_re(&send_blocks, cfg, recv) {
        Ok((map, lines, fields, fieldskipped, lines_skipped)) => (map, lines, fields, fieldskipped, lines_skipped),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!("{}", err_msg);
        }
    }
}

fn reused_str_vec(idx: usize, v: &mut Vec<String>, s: &str) {
    if idx < v.len() {
        v[idx].clear();
        v[idx].push_str(s);
    } else {
        v.push(String::from(s));
    }
}

fn grow_str_vec_or_add(idx: usize, v: &mut Vec<String>, s: &str, line: &str, linecount: usize, i: usize, verbose: usize) {
    if idx < v.len() {
        if verbose > 1 && v[idx].len() > 0 {
            eprintln!("idx: {} pushing more: {} to existing: {}  line: {} : {}  match# {}", idx, s, v[idx], line, linecount, i);
        }
        v[idx].push_str(s);
    } else {
        v.push(String::from(s));
    }
}

fn _worker_multi_re(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize, usize, usize), Box<dyn std::error::Error>> {
    let mut map = create_map();

    let mut re_es = vec![];
    //let mut cls = vec![];
    for r in &cfg.re_str {
        let re = match Regex_pre2::new(r) {
            Err(err) => panic!("Cannot parse regular expression {}, error = {}", r, err),
            Ok(r) => r,
        };

        re_es.push(re);
    }

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut fieldskipped = 0;
    let mut lines_skipped = 0;
    let mut rowcount = 0;
    let mut linecount = 0;
    let mut _skipped = 0;
    let mut re_curr_idx = 0;
    let mut acc_record: Vec<String> = vec![];

    let mut sch = if cfg.sample_schema.is_some() {
        Some(SchemaSample::new(cfg.sample_schema.unwrap()))
    } else {
        None
    };

    acc_record.push(String::new()); // push 1st blank whole match
    let mut cl = re_es[0].capture_locations();

    let mut latin_str_buff = String::with_capacity(1024);

    let mut acc_idx = 1usize;
    'BLOCK_LOOP: loop {
        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
                break;
            }
        };

        let convert = false;

        let lbuff = if convert {
            latin_str_buff.clear();
            fc.block[0..fc.len].iter().map(|&c| c as char).for_each(|c| latin_str_buff.push(c));
            latin_str_buff.as_bytes()
        } else {
            &fc.block[0..fc.len]
        };

        if fc.index == 0 {
            re_curr_idx = 0;
            for s in &mut acc_record {
                s.clear();
            }
            acc_idx = 1;
        } // start new file - start at first RE
        if cfg.verbose > 1 {
            eprintln!("file slice index: filename: {} index: {} len: {}", fc.filename, fc.index, fc.len);
        }

        if !cfg.noop_proc {
            'LINE_LOOP: for line in lbuff.lines() {
                if let Err(e) = line {
                    eprintln!("bad line");
                    continue 'LINE_LOOP;
                }
                let line = line?;

                linecount += 1;
                let re = &re_es[re_curr_idx];
                if let Some(_record) = re.captures_read(&mut cl, line.as_bytes())? {
                    let cw = CapWrap { cl: &cl, text: line.as_str() };
                    for i in 1..cw.len() {
                        let f = &cw[i];
                        acc_idx += 1;
                        grow_str_vec_or_add(acc_idx, &mut acc_record, f, &line, linecount, i, cfg.verbose);
                        if cfg.verbose > 2 {
                            eprintln!("mRE MATCHED: {}  REC: {:?}", line, acc_record);
                        }
                    }
                    re_curr_idx += 1;
                    if re_curr_idx >= re_es.len() {
                        let mut v: Vec<&str> = Vec::with_capacity(acc_record.len() + fc.sub_grps.len());
                        fc.sub_grps.iter()
                            .map(|x| x.as_str())
                            .chain(acc_record.iter().map(|x| x.as_str()))
                            .for_each(|x| v.push(x));
                        match sch {
                            Some(ref mut sch) => {
                                sch.schema_rec(&v, v.len());
                                if sch.done() {
                                    break 'BLOCK_LOOP;
                                }
                            }
                            _ => {
                                let (fc, fs, ls) = store_rec(&mut buff, "", &v, v.len(), &mut map, &cfg, &mut rowcount);
                                fieldcount += fc;
                                fieldskipped += fs;
                                lines_skipped += ls;
                            }
                        }
                        if cfg.verbose > 2 {
                            eprintln!("mRE STORE {:?} on line: {}", acc_record, line);
                        }

                        re_curr_idx = 0;
                        acc_record.iter_mut().for_each(move |s| s.clear());
                        for s in &mut acc_record {
                            s.clear();
                        }
                        acc_idx = 0;
                    }
                } else {
                    _skipped += 1;
                }
            }
        }
        if !cfg.recycle_io_blocks_disable {
            send_blocks.send(fc.block)?;
        }
    }
    if let Some(mut sch) = sch { sch.print_schema(&cfg); }
    Ok((map, rowcount, fieldcount, fieldskipped, lines_skipped))
}

fn ascii_line_fixup(slice: &[u8], start: usize, str: &mut String) {
    let mut pos = start;
    str.clear();
    loop {
    let b = slice[pos];
        if b == 10 || b == 11 {
            break;
        } else if b > 127 {
            str.push_str(format!("[0x{:x}]", b).as_str());
        } else {
            str.push(b as char);
        }
        pos += 1;
    }
}

fn ascii_line_fixup_full(slice: &[u8], str: &mut String) {
    str.clear();
    for b in slice {
        if *b == 10 || *b == 11 {
            break;
        } else if *b > 127 {
            str.push_str(format!("[0x{:x}]", b).as_str());
        } else {
            str.push(*b as char);
        }
    }
}

fn create_csv_builder(cfg: &CliCfg) -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(cfg.delimiter as u8).has_headers(cfg.skip_header).flexible(true);

    if cfg.quote.is_some() {
        builder.quote(cfg.quote.unwrap() as u8);
    }

    if cfg.escape.is_some() {
        builder.escape(Some(cfg.escape.unwrap() as u8));
    }

    if cfg.comment.is_some() {
        builder.comment(Some(cfg.comment.unwrap() as u8));
    }
    builder
}
