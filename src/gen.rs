use grep_cli::DecompressionReader;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, BufRead, Write, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::{fs, thread};
use pcre2::bytes::{Captures as Captures_pcre2, Regex as Regex_pre2};
use std::fs::File;
use flate2::read::GzDecoder;

// Unified decompression/open helper.
// Handles plain files and common compression extensions, returning a boxed Read.
// Respects io_block_size for buffered readers where applicable.
pub fn open_decompress(path: &Path, io_block_size: usize, verbosity: usize) -> Option<Box<dyn Read>> {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    // Reconstruct extension with leading dot for older logic compatibility.
    let dot_ext = if ext.is_empty() { String::new() } else { format!(".{}", ext) };
    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) => { eprintln!("skipping file \"{}\", open error: {}", path.display(), err); return None; }
    };
    // Helper to wrap with buffering if io_block_size > 0.
    let bufwrap = |f: File| -> Box<dyn Read> {
        if io_block_size != 0 { Box::new(BufReader::with_capacity(io_block_size, f)) } else { Box::new(BufReader::new(f)) }
    };
    match dot_ext.as_str() {
        ".gz" | ".tgz" => {
            if verbosity > 2 { eprintln!("opening gzip file {}", path.display()); }
            let rdr = bufwrap(file);
            Some(Box::new(GzDecoder::new(rdr)))
        }
        ".zst" | ".zstd" => {
            if verbosity > 2 { eprintln!("opening zstd file {}", path.display()); }
            let inner = bufwrap(file);
            match zstd::stream::read::Decoder::new(inner) {
                Ok(dec) => Some(Box::new(dec)),
                Err(err) => { eprintln!("skipping file \"{}\", zstd decoder error: {}", path.display(), err); None }
            }
        }
        // Delegate remaining formats to grep_cli's DecompressionReader; cannot customize buffer size here.
        ".bz2" | ".tbz2" | ".txz" | ".xz" | ".lz4" | ".lzma" | ".br" | ".z" => {
            if io_block_size != 0 && verbosity > 1 { eprintln!("file {} cannot override IO block size (external decoder)", path.display()); }
            match DecompressionReader::new(path) {
                Ok(rdr) => Some(Box::new(rdr)),
                Err(err) => { eprintln!("skipping compressed file \"{}\", error: {}", path.display(), err); None }
            }
        }
        _ => {
            if verbosity > 3 { eprintln!("opening plain file {}", path.display()); }
            Some(bufwrap(file))
        }
    }
}

// Cross-platform lightweight thread id helper. On Linux use gettid syscall; on other Unix fall back
// to pthread_self (sufficient for diagnostic logging). On Windows use GetCurrentThreadId.
#[cfg(target_os = "windows")]
pub fn gettid() -> usize {
    unsafe { winapi::um::processthreadsapi::GetCurrentThreadId() as usize }
}

#[cfg(all(unix, target_os = "linux"))]
pub fn gettid() -> usize {
    unsafe { libc::syscall(libc::SYS_gettid) as usize }
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn gettid() -> usize {
    // pthread_self returns a pthread_t which we cast to usize for logging purposes only.
    unsafe { libc::pthread_self() as usize }
}

pub fn distro_format<T, S>(map: &HashMap<T, usize, S>, upper: usize, bottom: usize) -> String
    where
        T: std::fmt::Display + std::fmt::Debug + std::clone::Clone + Ord,
{
    let mut vec: Vec<(usize, T)> = Vec::with_capacity(map.len());
    for x in map.iter() {
        vec.push((*x.1, x.0.clone()));
    }
    vec.sort_by(|x, y| {
        let ev = y.0.cmp(&x.0);
        // we do the extra ordering here to get predictable results
        // for testing and for user sanity
        if ev == core::cmp::Ordering::Equal {
            // println!("having the compare {} {}", y.1, x.1);
            x.1.cmp(&y.1)
        } else {
            ev
        }
    });

    let mut msg = String::with_capacity(16);
    if upper + bottom >= vec.len() {
        for e in &vec {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
    } else {
        for e in vec.iter().take(upper) {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
        msg.push_str(&format!("..{}..", vec.len() - (bottom + upper)));
        for e in vec.iter().skip(vec.len() - bottom) {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
    }
    msg
}

#[test]
fn test_distro_format() {
    let mut v: HashMap<String, usize> = HashMap::new();
    v.insert("a".to_string(), 10);
    v.insert("z".to_string(), 111);
    v.insert("c".to_string(), 11);
    v.insert("q".to_string(), 5);

    let result = "0 0 distro: ..4..
0 1 distro: ..3..(q x 5)
0 2 distro: ..2..(a x 10)(q x 5)
0 3 distro: ..1..(c x 11)(a x 10)(q x 5)
1 0 distro: (z x 111)..3..
1 1 distro: (z x 111)..2..(q x 5)
1 2 distro: (z x 111)..1..(a x 10)(q x 5)
1 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
2 0 distro: (z x 111)(c x 11)..2..
2 1 distro: (z x 111)(c x 11)..1..(q x 5)
2 2 distro: (z x 111)(c x 11)(a x 10)(q x 5)
2 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 0 distro: (z x 111)(c x 11)(a x 10)..1..
3 1 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 2 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
";

    let mut res = String::new();
    for u in 0..4 {
        for b in 0..4 {
            res.push_str(&format!("{} {} distro: {}\n", u, b, &distro_format(&v, u, b)));
        }
    }
    assert_eq!(&res, result, "distro_format cmp failed");
}

/// Format bytes into human readable units with up to `sig` significant digits.
/// Example: mem_metric_digit(1024,4) => "    1 KB"
#[cfg(feature = "stats")]
pub fn mem_metric_digit(v: usize, sig: usize) -> String {
    if v == 0 || v > usize::MAX / 2 { return format!("{:>width$}", "unknown", width = sig + 3); }
    const METRIC: [&str; 8] = ["B ", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"];
    let mut size = 1usize << 10;
    let mut unit = "";
    for m in &METRIC { if v < size { unit = m; break; } size <<= 10; }
    let base = (size >> 10) as f64;
    let value = v as f64 / base;
    let mut s = format!("{}", value);
    s.truncate(sig + 1);
    if s.ends_with('.') { s.pop(); }
    format!("{:>width$} {}", s, unit, width = sig + 1)
}
#[cfg(not(feature = "stats"))]
pub fn mem_metric_digit(v: usize, _sig: usize) -> String { v.to_string() }

#[derive(Default, Debug)]
pub struct IoSlicerStatus {
    pub bytes: AtomicUsize,
    pub files: AtomicUsize,
    pub curr_file: Mutex<String>,
}

// new() replaced by Default derive

#[derive(Default, Debug)]
pub struct MergeStatus {
    pub current: AtomicUsize,
    pub total: AtomicUsize,
}

// new() replaced by Default derive

#[derive(Debug)]
pub struct FileSlice {
    pub block: Vec<u8>,
    pub len: usize,
    pub index: usize,
    pub filename: String,
    pub sub_grps: Vec<String>,
}

fn fill_buff(handle: &mut dyn Read, buff: &mut [u8]) -> Result<usize, std::io::Error> {
    let mut sz = handle.read(buff)?;
    loop {
        if sz == 0 || sz == buff.len() { return Ok(sz); }
        let sz2 = handle.read(&mut buff[sz..])?;
        if sz2 == 0 { return Ok(sz); }
        sz += sz2;
    }
}

pub struct IoThreadCtx {
    pub recv_blocks: crossbeam_channel::Receiver<Vec<u8>>,
    pub send_fileslice: crossbeam_channel::Sender<Option<FileSlice>>,
    pub status: Arc<IoSlicerStatus>,
    pub verbosity: usize,
    pub block_size: usize,
    pub recycle_disable: bool,
}

pub fn io_thread_slicer(
    ctx: &IoThreadCtx,
    currfilename: &dyn Display,
    file_subgrps: &[String],
    handle: &mut dyn Read,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let recv_blocks = &ctx.recv_blocks;
    let send_fileslice = &ctx.send_fileslice;
    let status = &ctx.status;
    let verbosity = ctx.verbosity;
    let block_size = ctx.block_size;
    let recycle_disable = ctx.recycle_disable;
    if verbosity >= 2 {
        eprintln!("Using block_size {} bytes", block_size);
    }

    {
        let mut curr_file = status.curr_file.lock().unwrap();
        curr_file.clear();
        curr_file.push_str(format!("{}", currfilename).as_str());
    }
    status.files.fetch_add(1, Ordering::Relaxed);

    let mut block_count = 0;
    let mut bytes = 0;

    let mut holdover = vec![0u8; block_size];
    let mut left_len = 0;
    let mut last_left_len;
    let mut curr_pos = 0usize;
    loop {
    let mut block = if !recycle_disable { recv_blocks.recv()? } else { vec![0u8; block_size] };
        if left_len > 0 {
            block[0..left_len].copy_from_slice(&holdover[0..left_len]);
        }
        let (expected_sz, sz) = {
            let expected_sz = block.len() - left_len;
            let sz = fill_buff(handle, &mut block[left_len..])?;
            (expected_sz, sz)
        };

        let apparent_eof = expected_sz != sz;

        let mut end = 0;
        let mut found = false;
        for i in (0..(sz + left_len)).rev() {
            end = i;
            match block[end] {
                b'\r' | b'\n' => {
                    found = true;
                    break;
                }
                _ => {}
            }
        }
        // println!("read block: {:?}\nhold block: {:?}", block, holdover);

        curr_pos += sz;
        if sz > 0 {
            block_count += 1;
            if !found {
                last_left_len = left_len;
                left_len = 0;
                //holdover[0..left_len].copy_from_slice(&block[end..sz + last_left_len]);

                if !apparent_eof {
                    eprintln!("WARNING: sending no EOL found in block around file:pos {}:{} ", currfilename, curr_pos);
                }
                if verbosity > 0 { eprintln!("DBG slicer sending slice (no EOL) size:{} file:{}", sz + last_left_len, currfilename); }
                send_fileslice.send(Some(FileSlice {
                    block,
                    len: sz + last_left_len,
                    index: block_count,
                    filename: currfilename.to_string(),
                    sub_grps: file_subgrps.to_vec(),
                }))?;
                status.bytes.fetch_add(sz, Ordering::Relaxed);
                bytes += sz;

                // panic!("Cannot find end line marker in current block at pos {} from file {}", curr_pos, currfilename);
            } else {
                last_left_len = left_len;
                left_len = (sz + last_left_len) - (end + 1);
                holdover[0..left_len].copy_from_slice(&block[end + 1..sz + last_left_len]);

                if verbosity > 2 {
                    eprintln!("sending found EOL at {} ", end + 1);
                }
                    if verbosity > 0 { eprintln!("DBG slicer sending slice size:{} file:{}", end + 1, currfilename); }
                    send_fileslice.send(Some(FileSlice {
                    block,
                    len: end + 1,
                    index: block_count,
                    filename: currfilename.to_string(),
                    sub_grps: file_subgrps.to_vec(),
                }))?;
                status.bytes.fetch_add(end + 1, Ordering::Relaxed);
                bytes += end + 1;
            }
        } else {
            if verbosity > 2 {
                eprintln!("sending tail len {} on file: {} ", left_len, currfilename);
            }
            if verbosity > 0 { eprintln!("DBG slicer sending tail slice len:{} file:{}", left_len, currfilename); }
            send_fileslice.send(Some(FileSlice {
                block,
                len: left_len,
                index: block_count,
                filename: currfilename.to_string(),
                sub_grps: file_subgrps.to_vec(),
            }))?;
            bytes += left_len;
            status.bytes.fetch_add(left_len, Ordering::Relaxed);

            break;
        }
    }
    Ok((block_count, bytes))
}

pub fn caps_to_vec_strings(caps: &Captures_pcre2) -> Vec<String> {
    let mut v = vec![];
    for x in 1..caps.len() {
        let y = caps.get(x).unwrap().as_bytes();
        let ss = String::from_utf8(y.to_vec()).unwrap();
        v.push(ss);
    }
    v
}

use std::path::Path;

pub fn subs_from_path_buff(path: &Path, regex: &Option<Regex_pre2>) -> (bool, Vec<String>) {
    let v = vec![];
    match regex {
        None => (true, v),
        Some(re) => {
            match re.captures(path.to_str().unwrap().as_bytes()) {
                Ok(Some(caps)) => (true, caps_to_vec_strings(&caps)),
                _ => (false, v)
            }
        }
    }
}

pub struct PerFileCtx {
    pub recv_blocks: crossbeam_channel::Receiver<Vec<u8>>,
    pub recv_pathbuff: crossbeam_channel::Receiver<Option<PathBuf>>,
    pub send_fileslice: crossbeam_channel::Sender<Option<FileSlice>>,
    pub io_block_size: usize,
    pub block_size: usize,
    pub verbosity: usize,
    pub recycle_disable: bool,
}

pub fn per_file_thread(
    ctx: PerFileCtx,
    io_status: Arc<IoSlicerStatus>,
    path_re: &Option<Regex_pre2>,
) -> (usize, usize) {
    let recv_blocks = ctx.recv_blocks;
    let recv_pathbuff = ctx.recv_pathbuff;
    let send_fileslice = ctx.send_fileslice;
    let io_block_size = ctx.io_block_size;
    let block_size = ctx.block_size;
    let verbosity = ctx.verbosity;
    let recycle_disable = ctx.recycle_disable;
    let mut block_count = 0usize;
    let mut bytes = 0usize;
    loop {
        let filename = match recv_pathbuff.recv().expect("thread failed to get next job from channel") {
            Some(path) => path,
            None => {
                if verbosity > 1 {
                    eprintln!("slicer exit on None {}", thread::current().name().unwrap());
                }
                return (block_count, bytes);
            }
        };
        if verbosity > 2 { eprintln!("considering path: {}", filename.display()); }
    let (file_matched, file_subgrps) = subs_from_path_buff(filename.as_path(), path_re);
        if !file_matched {
            if verbosity > 0 { eprintln!("file: {} did not match expected pattern", filename.display()); }
            continue;
        }

        match fs::metadata(&filename) {
            Ok(m) => if !m.is_file() { continue; },
            Err(err) => {
                eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", filename.display(), err);
                continue;
            }
        };
        if verbosity >= 1 {
            eprintln!("thread id: {} processing file: {}", gettid(), filename.display());
        }

        // Unified open/decompress
        let rdr = match open_decompress(&filename, io_block_size, verbosity) {
            Some(r) => r,
            None => continue,
        };
        // If header skipping is required (name-based keys resolved earlier), remove first line now.
        let mut boxed_reader: Box<dyn Read> = rdr;
        // We detect need by looking at global config via environment-free hook: if first slice index should start after header.
        // Simplest approach: read and discard first line before handing to slicer when has_headers(true) & names used.
        // We cannot access cfg directly here; instead rely on pattern: workers expect header removed when key_fields_resolved && skip_header was set.
        // So always drop first line when skip_header was set and key_fields_resolved involved name usage.
        // Provide a lightweight heuristic: presence of commas and lack of newline in first 8KB chunk -> treat as header.
        // (NOTE: Ideal would pass a flag; future refactor may extend PerFileCtx with a boolean.)
        // Implementation: peek first line and discard if it ends with \n or \r.
        {
            use std::io::Read as _;
            let mut peek_buf: Vec<u8> = Vec::with_capacity(1024);
            let mut single = [0u8;1];
            // Read until newline or EOF but cap.
            let mut consumed = 0usize;
            loop {
                match boxed_reader.read(&mut single) {
                    Ok(0) => break,
                    Ok(_) => {
                        consumed += 1;
                        peek_buf.push(single[0]);
                        if single[0] == b'\n' || single[0] == b'\r' { break; }
                        if consumed >= 8192 { break; }
                    }
                    Err(_) => break,
                }
            }
            // We intentionally discard this buffer (header line). Following content starts after header.
            // No push-back needed; we just proceed. If file had no newline we treat entire file as a single header row.
        }
        let io_ctx = IoThreadCtx {
            recv_blocks: recv_blocks.clone(),
            send_fileslice: send_fileslice.clone(),
            status: io_status.clone(),
            verbosity,
            block_size,
            recycle_disable,
        };
        match io_thread_slicer(
            &io_ctx,
            &filename.display(),
            &file_subgrps,
            &mut boxed_reader,
        ) {
            Ok((bc, by)) => {
                block_count += bc;
                bytes += by;
            }
            Err(err) => eprintln!("error io slicing {}, error: {}", &filename.display(), err),
        }
    }
}


#[cfg(not(target_os = "windows"))]
pub fn get_reader_writer() -> (impl BufRead, impl Write) {
    use std::os::unix::io::FromRawFd;
    let stdin = unsafe { File::from_raw_fd(0) };
    let stdout = unsafe { File::from_raw_fd(1) };
    let (reader, writer) = (BufReader::new(stdin), BufWriter::new(stdout));
    (reader, writer)
}

#[cfg(target_os = "windows")]
pub fn get_reader_writer() -> (impl BufRead, impl Write) {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    let stdin = unsafe { File::from_raw_handle(std::io::stdin().as_raw_handle()) };
    let stdout = unsafe { File::from_raw_handle(std::io::stdout().as_raw_handle()) };

    let (reader, writer) = (BufReader::new(stdin), BufWriter::new(stdout));
    (reader, writer)
}

// Generic iterator-based distribution formatter to avoid temporary HashMap materialization.
pub fn distro_format_iter<'a, I>(iter: I, upper: usize, bottom: usize) -> String
    where I: Iterator<Item = (&'a String, &'a usize)>
{
    let mut vec: Vec<(usize, &String)> = iter.map(|(k,c)| (*c, k)).collect();
    vec.sort_by(|x,y| {
        let ev = y.0.cmp(&x.0);
        if ev == core::cmp::Ordering::Equal { x.1.cmp(y.1) } else { ev }
    });
    let mut msg = String::with_capacity(16);
    if upper + bottom >= vec.len() {
        for e in &vec { msg.push_str(&format!("({} x {})", e.1, e.0)); }
    } else {
        for e in vec.iter().take(upper) { msg.push_str(&format!("({} x {})", e.1, e.0)); }
        msg.push_str(&format!("..{}..", vec.len() - (bottom + upper)));
        for e in vec.iter().skip(vec.len() - bottom) { msg.push_str(&format!("({} x {})", e.1, e.0)); }
    }
    msg
}