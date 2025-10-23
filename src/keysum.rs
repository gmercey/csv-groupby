use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::time::Instant;
use rayon::prelude::*;
use crate::cli::CliCfg;
use crate::{KEY_DEL, MyMap};
use crate::gen::MergeStatus;
use std::cmp::{min, max};
use prettytable::Table;
use itertools::Itertools;

#[derive(Debug)]
pub struct SchemaSample {
    pub matrix: Vec<Vec<String>>,
    pub num: u32,
}

// Aggregation record per key
const DISTINCT_SMALL_THRESHOLD: usize = 16; // max entries before upgrading to HashMap

#[derive(Debug)]
pub enum DistinctStore {
    Small(Vec<(String, usize)>), // simple vec for small cardinalities
    #[cfg(feature = "fast-hash")]
    Map(hashbrown::HashMap<String, usize, ahash::RandomState>),
    #[cfg(not(feature = "fast-hash"))]
    Map(HashMap<String, usize>),
}

impl DistinctStore {
    fn new() -> Self { DistinctStore::Small(Vec::new()) }
    pub fn len(&self) -> usize { match self { DistinctStore::Small(v) => v.len(), DistinctStore::Map(m) => m.len() } }
    fn ensure_map(&mut self) {
        if let DistinctStore::Small(v) = self {
            if v.len() > DISTINCT_SMALL_THRESHOLD {
                #[cfg(feature = "fast-hash")]
                let mut m: hashbrown::HashMap<String, usize, ahash::RandomState> = hashbrown::HashMap::with_capacity_and_hasher(v.len()*2, ahash::RandomState::default());
                #[cfg(not(feature = "fast-hash"))]
                let mut m: HashMap<String, usize> = HashMap::with_capacity(v.len()*2);
                for (k,c) in v.drain(..) { m.insert(k,c); }
                *self = DistinctStore::Map(m);
            }
        }
    }
    pub fn insert_inc(&mut self, key: &str) {
        match self {
            DistinctStore::Small(v) => {
                for &mut (ref mut k, ref mut c) in v.iter_mut() {
                    if k == key { *c += 1; return; }
                }
                v.push((key.to_string(), 1));
                if v.len() > DISTINCT_SMALL_THRESHOLD { self.ensure_map(); }
            }
            DistinctStore::Map(m) => {
                m.entry(key.to_string()).and_modify(|c| *c += 1).or_insert(1);
            }
        }
    }
    pub fn merge_into(&mut self, other: DistinctStore) {
        match other {
            DistinctStore::Small(v) => {
                for (k,c) in v { self.add_count(k, c); }
            }
            DistinctStore::Map(m) => {
                for (k,c) in m { self.add_count(k, c); }
            }
        }
    }
    fn add_count(&mut self, key: String, add: usize) {
        match self {
            DistinctStore::Small(v) => {
                for &mut (ref mut k, ref mut c) in v.iter_mut() {
                    if *k == key { *c += add; return; }
                }
                v.push((key, add));
                if v.len() > DISTINCT_SMALL_THRESHOLD { self.ensure_map(); }
            }
            DistinctStore::Map(m) => {
                m.entry(key).and_modify(|c| *c += add).or_insert(add);
            }
        }
    }
    pub fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&'a String, &'a usize)> + 'a> {
        match self {
            DistinctStore::Small(v) => Box::new(v.iter().map(|(k,c)| (k,c))),
            DistinctStore::Map(m) => Box::new(m.iter()),
        }
    }
}

pub struct KeySum {
    pub count: u64,
    pub nums: Vec<Option<f64>>,      // numeric aggregates: sum/min/max slots in order
    pub strs: Vec<Option<String>>,   // string min/max slots
    pub avgs: Vec<(f64, usize)>,     // (running total, count) aligned to avg_specs ordering
    pub distinct: Vec<DistinctStore>, // distinct value counts per unique field
}

impl KeySum {
    pub fn new(num_len: usize, strs_len: usize, dist_len: usize, avg_len: usize) -> KeySum {
        KeySum {
            count: 0,
            nums: vec![None; num_len],
            strs: vec![None; strs_len],
            avgs: vec![(0f64, 0usize); avg_len],
            distinct: (0..dist_len).map(|_| DistinctStore::new()).collect(),
        }
    }
    pub fn from_cfg(cfg: &CliCfg) -> KeySum {
        KeySum::new(
            cfg.num_specs.len(),
            cfg.str_specs.len(),
            cfg.unique_fields.len(),
            cfg.avg_specs.len(),
        )
    }
}

impl SchemaSample {
    pub fn new(num: u32) -> Self {
        SchemaSample { matrix: vec![], num }
    }
    pub fn schema_rec<T>(&mut self, record: &T, rec_len: usize)
    where
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
        T: std::ops::Index<usize> + std::fmt::Debug,
    {
        if self.matrix.is_empty() {
            for i in 1..=rec_len {
                self.matrix.push(vec![i.to_string()]);
            }
        }
        for i in 0..rec_len {
            if self.matrix.get(i).is_none() {
                self.matrix.push(vec![i.to_string()]);
            }
            self.matrix
                .get_mut(i)
                .expect("matrix slot must exist")
                .push(record.index(i).as_ref().to_string());
        }
    }
    pub fn print_schema(&mut self, cfg: &CliCfg) {
        if self.matrix.is_empty() { return; }
        let mut header = vec!["index".to_string()];
        let num_cols = self.matrix.first().unwrap().len();
        for c in 1..num_cols { header.push(format!("line{}", c)); }
        self.matrix.insert(0, header);
        let mut padding: Vec<usize> = vec![];
        for row in &self.matrix {
            for (c, cell) in row.iter().enumerate() {
                let this_len = cell.len();
                if let Some(p) = padding.get_mut(c) {
                    *p = (*p).max(this_len);
                } else {
                    padding.push(this_len);
                }
            }
        }
        for row in &self.matrix {
            let num_cols = row.len();
            for (c, cell) in row.iter().enumerate() {
                print!("{:>width$}", cell, width = padding[c]);
                if c < num_cols - 1 { print!("{} ", cfg.od); } else { println!(); }
            }
        }
        std::process::exit(0);
    }
    pub fn done(&self) -> bool {
        !self.matrix.is_empty() && self.matrix.first().unwrap().len() > self.num as usize
    }
}
// (Removed legacy duplicate print_schema/done implementations)

// Removed legacy free-function schema_rec (now using SchemaSample::schema_rec method).

pub fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> (usize,usize,usize)
    where
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
        T: std::ops::Index<usize> + std::fmt::Debug,
{
    //let mut ss: String = String::with_capacity(256);
    // Reusable key buffer: clear then pre-reserve exact space needed to avoid incremental reallocations
    ss.clear();
    {
        let key_fields_guard = cfg.key_fields();
        if !key_fields_guard.is_empty() {
            // Estimate required length = sum(len(field or null)) + delimiters (len - 1)
            let mut needed = 0usize;
            for (i, kf) in key_fields_guard.iter().enumerate() {
                if *kf < rec_len { needed += record[*kf].as_ref().len(); } else { needed += cfg.null.len(); }
                if i + 1 < key_fields_guard.len() { needed += 1; } // KEY_DEL char
            }
            if ss.capacity() < needed { ss.reserve(needed - ss.capacity()); }
        } else if ss.capacity() < cfg.null.len() {
            ss.reserve(cfg.null.len() - ss.capacity());
        }
    }

    let mut fieldcount = 0usize;
    let mut skip_parse_fields = 0usize;
    let lines_filtered = 0usize;
    use pcre2::bytes::{CaptureLocations as CaptureLocations_pcre2, Captures as Captures_pcre2, Regex as Regex_pre2};

    if cfg.verbose >= 3 {
    if !line.is_empty() {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }

    // Predicate short-circuit fusion: combine where and where_not checks into single pass.
    // Build a small unified slice of (field_index, regex, mode) where mode=true means positive match required; false means match must NOT occur.
    if cfg.where_re.is_some() || cfg.where_not_re.is_some() {
        // Fast path: allocate temporary Vec only if both are present; otherwise iterate directly.
        if cfg.where_re.is_some() && cfg.where_not_re.is_some() {
            // Both sets present: build fused list then evaluate.
            let mut fused: Vec<(usize, &pcre2::bytes::Regex, bool)> = Vec::new();
            if let Some(ref w) = cfg.where_re { for (fi,re) in w { fused.push((*fi, re, true)); } }
            if let Some(ref wn) = cfg.where_not_re { for (fi,re) in wn { fused.push((*fi, re, false)); } }
            for (fi, re, mode) in fused {
                if fi < rec_len {
                    match re.is_match(record[fi].as_ref().as_bytes()) {
                        Err(e) => eprintln!("error trying to match \"{}\" with RE \"{}\" for {} check, error: {}", record[fi].as_ref(), re.as_str(), if mode {"where"} else {"where_not"}, &e),
                        Ok(matched) => {
                            // For positive mode require match; for negative mode require NO match.
                            if (mode && !matched) || (!mode && matched) { return (0,0,1); }
                        }
                    }
                }
            }
        } else if let Some(ref w) = cfg.where_re { // Only positive predicates
            for (fi, re) in w {
                if *fi < rec_len {
                    match re.is_match(record[*fi].as_ref().as_bytes()) {
                        Err(e) => eprintln!("error trying to match \"{}\" with RE \"{}\" for where check, error: {}", record[*fi].as_ref(), re.as_str(), &e),
                        Ok(b) => if !b { return (0,0,1); },
                    }
                }
            }
        } else if let Some(ref wn) = cfg.where_not_re { // Only negative predicates
            for (fi, re) in wn {
                if *fi < rec_len {
                    match re.is_match(record[*fi].as_ref().as_bytes()) {
                        Err(e) => eprintln!("error trying to match \"{}\" with RE \"{}\" for where_not check, error: {}", record[*fi].as_ref(), re.as_str(), &e),
                        Ok(b) => if b { return (0,0,1); },
                    }
                }
            }
        }
    }

    let key_fields_guard = cfg.key_fields();
    if !key_fields_guard.is_empty() {
        fieldcount += rec_len;
        for i in 0..key_fields_guard.len() {
            let index = key_fields_guard[i];
            if index < rec_len {
                // re.is_match(&record[index].as_ref().as_bytes());
                ss.push_str(record[index].as_ref());
            } else {
                ss.push_str(&cfg.null);
            }
            ss.push(KEY_DEL);
        }
        ss.pop(); // remove the trailing KEY_DEL instead of check each iteration
        // we know we get here because of the if above.
    } else {
        ss.push_str(&cfg.null);
    }
    *rowcount += 1;

    let brec: &mut KeySum = {
        if let Some(v1) = map.get_mut(ss) { v1 } else {
            // Use spec-based sizing
            let v2 = KeySum::from_cfg(cfg);
            map.insert(ss.clone(), v2);
            map.get_mut(ss).unwrap()
        }
    };

    brec.count += 1;

    // Unified numeric aggregation via specs
    if !cfg.num_specs.is_empty() {
        for spec in &cfg.num_specs {
            if spec.src_index >= rec_len { continue; }
            let raw = record[spec.src_index].as_ref();
            match raw.parse::<f64>() {
                Ok(val) => {
                    let slot = &mut brec.nums[spec.dest_index];
                    *slot = Some(match (&*slot, spec.kind) {
                        (Some(curr), crate::cli::NumOpKind::Sum) => *curr + val,
                        (None, crate::cli::NumOpKind::Sum) => val,
                        (Some(curr), crate::cli::NumOpKind::Min) => curr.min(val),
                        (None, crate::cli::NumOpKind::Min) => val,
                        (Some(curr), crate::cli::NumOpKind::Max) => curr.max(val),
                        (None, crate::cli::NumOpKind::Max) => val,
                    });
                }
                Err(_) => {
                    skip_parse_fields += 1;
                    if cfg.verbose > 1 {
                        eprintln!("Error parsing numeric field '{}' (spec {:?}) - skipping", raw, spec.kind);
                    }
                }
            }
        }
    }

    if !cfg.avg_specs.is_empty() {
        for spec in &cfg.avg_specs {
            if spec.src_index < rec_len {
                let v = &record[spec.src_index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        skip_parse_fields += 1;
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for avg index: {} so pretending value is 0", v.as_ref(), spec.src_index);
                        }
                    }
                    Ok(vv) => {
                        brec.avgs[spec.dest_index].0 += vv;
                        brec.avgs[spec.dest_index].1 += 1;
                    }
                }
            }
        }
    }
    if !cfg.unique_fields.is_empty() {
        for i in 0..cfg.unique_fields.len() {
            let index = cfg.unique_fields[i];
            if index < rec_len {
                brec.distinct[i].insert_inc(record[index].as_ref());
            }
        }
    }

    // Unified string aggregation via specs
    if !cfg.str_specs.is_empty() {
        for spec in &cfg.str_specs {
            if spec.src_index >= rec_len { continue; }
            let vref = record[spec.src_index].as_ref();
            let slot = &mut brec.strs[spec.dest_index];
            match spec.kind {
                crate::cli::StrOpKind::Min => {
                    match slot {
                        Some(curr) => { if curr.as_str() > vref { curr.clear(); curr.push_str(vref); } },
                        None => *slot = Some(vref.to_string()),
                    }
                }
                crate::cli::StrOpKind::Max => {
                    match slot {
                        Some(curr) => { if curr.as_str() < vref { curr.clear(); curr.push_str(vref); } },
                        None => *slot = Some(vref.to_string()),
                    }
                }
            }
        }
    }

    (fieldcount,skip_parse_fields, lines_filtered)
}

fn merge_f64<F>(x: Option<f64>, y: Option<f64>, pickone: F) -> Option<f64>
    where F: Fn(f64, f64) -> f64
{
    match (x, y) {
        (Some(old), Some(new)) => Some(pickone(old, new)),
        (Some(old), None) => Some(old),
        (None, Some(new)) => Some(new),
        (_, _) => None,
    }
}

fn merge_string<'a, F>(old: &'a mut Option<String>, new: &'a mut Option<String>, pickone: F)
    where F: Fn(&'a mut Option<String>, &'a mut Option<String>)
{
    match (&old, &new) {
        (Some(_), Some(_)) => pickone(old,new), //*new = old.take(), //f(new,old),
        (Some(_), None) => *new = old.take(),
        (None, Some(_)) => {}
        (_, _) => {}
    }
}

const MERGE_PAR_THRESHOLD_MAPS: usize = 4; // minimum number of maps to justify parallel merge
const MERGE_PAR_THRESHOLD_ENTRIES: usize = 10_000; // minimum total entries to justify parallel merge

pub fn sum_maps(maps: &mut Vec<MyMap>, verbose: usize, cfg: &CliCfg, merge_status: &mut Arc<MergeStatus>) -> MyMap {
    use itertools::join;
    let start = Instant::now();
    let lens = join(maps.iter().map(|x:&MyMap| x.len().to_string()), ",");
    let tot_merge_items:usize = maps.iter().map(|m| m.len()).sum();
    merge_status.total.store(tot_merge_items, std::sync::atomic::Ordering::Relaxed);
    merge_status.current.store(0, std::sync::atomic::Ordering::Relaxed);
    if maps.is_empty() { return MyMap::default(); }
    if maps.len() == 1 { return maps.remove(0); }

    // Pairwise merge function (sequential for a pair)
    let merge_pair = |mut left: MyMap, right: MyMap| -> MyMap {
    for (k, mut old) in right.into_iter() {
            merge_status.current.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let new = left.entry(k).or_insert(KeySum::new(old.nums.len(), old.strs.len(), old.distinct.len(), old.avgs.len()));
            new.count += old.count;
            for spec in &cfg.num_specs {
                let d = spec.dest_index;
                new.nums[d] = match spec.kind {
                    crate::cli::NumOpKind::Sum => merge_f64(new.nums[d], old.nums[d], |x,y| x + y),
                    crate::cli::NumOpKind::Min => merge_f64(new.nums[d], old.nums[d], |x,y| x.min(y)),
                    crate::cli::NumOpKind::Max => merge_f64(new.nums[d], old.nums[d], |x,y| x.max(y)),
                };
            }
            for j in 0..old.avgs.len() {
                new.avgs[j].0 += old.avgs[j].0;
                new.avgs[j].1 += old.avgs[j].1;
            }
            for spec in &cfg.str_specs {
                let d = spec.dest_index;
                match spec.kind {
                    crate::cli::StrOpKind::Min => {
                        merge_string(&mut new.strs[d], &mut old.strs[d], |new_slot, old_slot| {
                            if new_slot > old_slot { *new_slot = old_slot.take(); }
                        });
                    }
                    crate::cli::StrOpKind::Max => {
                        merge_string(&mut new.strs[d], &mut old.strs[d], |new_slot, old_slot| {
                            if new_slot < old_slot { *new_slot = old_slot.take(); }
                        });
                    }
                }
            }
            for j in 0..old.distinct.len() {
                let src = std::mem::replace(&mut old.distinct[j], DistinctStore::new());
                new.distinct[j].merge_into(src);
            }
        }
        left
    };

    let mut working: Vec<MyMap> = std::mem::take(maps);
    // Decide whether to use parallel reduction based on thresholds (after taking maps)
    let use_parallel = working.len() >= MERGE_PAR_THRESHOLD_MAPS || tot_merge_items >= MERGE_PAR_THRESHOLD_ENTRIES;
    while working.len() > 1 {
        if working.len() == 2 {
            let right = working.pop().unwrap();
            let left = working.pop().unwrap();
            working.push(merge_pair(left, right));
        } else if use_parallel {
            working = working
                .par_chunks_mut(2)
                .map(|chunk| {
                    if chunk.len() == 2 {
                        let right = std::mem::take(&mut chunk[1]);
                        let left = std::mem::take(&mut chunk[0]);
                        merge_pair(left, right)
                    } else {
                        std::mem::take(&mut chunk[0])
                    }
                })
                .collect();
        } else {
            // Sequential pairwise merge for small workloads
            let mut next: Vec<MyMap> = Vec::with_capacity(working.len().div_ceil(2));
            let mut iter = working.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    next.push(merge_pair(left, right));
                } else {
                    next.push(left);
                }
            }
            working = next;
        }
    }
    let result = working.pop().unwrap_or_default();
    let end = Instant::now();
    let dur = end - start;
    if verbose > 0 {
        eprintln!("merge maps time: {:.3}s from map entry counts: [{}] to single map {} entries{}", dur.as_millis() as f64 / 1000.0f64, lens, result.len(), if use_parallel { " (parallel)" } else { " (seq)" });
    }
    result
}

// Removed obsolete commented-out store_field prototype.
