# csv-groupby
## `gb`   A Command that does a SQL like "group by" on delimited files OR arbitrary lines of text

[![CI](https://github.com/sflanaga/csv-groupby/actions/workflows/ci.yml/badge.svg)](https://github.com/sflanaga/csv-groupby/actions/workflows/ci.yml)

gb (current: 0.20.0) is a command that takes delimited data (like csv files) or lines of text (like a log file) and
emulates a SQL style select .. group by over that data.  It is partially inspired by
[xsv](https://github.com/BurntSushi/xsv) and the desire to stop writing the same ad‑hoc perl/awk one‑liners
for large log analysis.

## 0.20.0 Highlights
- Migrated CLI from structopt to clap 4 (derive) – improved help formatting & future-proof flags
- Upgraded core dependencies (regex 1.10, crossbeam 0.8, prettytable-rs 0.10, crossterm 0.27, zstd 0.13, itertools 0.13, etc.)
- Added Criterion benchmark harness (`benches/basic.rs`) to start tracking performance trends
- Added pinned MSRV via `rust-toolchain.toml` (Rust 1.90.0)
- Initial clippy hygiene pass (many needless allocations & len() checks cleaned; further refinements queued)

## Feature Flags (modular build)
The binary now uses Cargo feature flags to let you opt into diagnostic overhead only when you need it.

Current features:

* `core-io` (default): Core parsing, regex/csv processing, decompression, aggregation logic.
* `stats` (default): Enables the live ticker and human friendly memory formatting used in the summary output.
* `memory-tracking` (opt‑in): Adds an allocator instrumentation layer to report approximate in‑process allocation totals. Slight throughput impact; disable for pure performance runs.

### Build combinations
Minimal (no diagnostics):
 - Refactored aggregation engine to a spec‑driven model (unified enum specs for numeric sum/min/max, string min/max, and avg) eliminating duplicated loops.
 - Introduced adaptive distinct counting `DistinctStore` (small inline vector that upgrades to a hash map above 16 unique values) reducing per‑key overhead for low‑cardinality fields.
 - Added pairwise parallel merge with thresholds (maps>=4 or total entries>=10k) to avoid rayon overhead on small workloads while accelerating large reductions.
 - Zero‑copy CSV output streaming (direct writer formatting via `itoa`/`ryu`) and suppressed trailing `.0` for integer float values to keep test output stable.
 - Optional fast hashing (`--features fast-hash`) switching key maps & large distinct maps to `hashbrown + ahash` for higher throughput (default BTreeMap remains for deterministic ordering when feature is off).
 - Predicate short‑circuit fusion combines `--where_re` and `--where_not_re` evaluation into one pass per record.
 - Memory instrumentation flag `--mem-stats` summarizing distinct store usage (small vs map slots, total distinct entries, avg entries/slot).
```
cargo build --release
```

All features (including memory tracking):
```
cargo build --release --features "stats memory-tracking"
```

Disable only stats (keep memory tracking for a targeted test):
```
cargo build --release --no-default-features --features "core-io memory-tracking"
* Without `stats`: No periodic ticker thread; summary prints raw byte counts without human digit grouping.
* With `stats`: Periodic progress line (records/sec, MB/s, memory); humanized memory sizes.
* With `memory-tracking`: Extra memory metrics collected; small overhead (generally <5%).

### When to toggle
* High throughput batch processing: Prefer disabling `stats` and `memory-tracking`.
* Interactive exploration / sizing runs: Enable `stats`.
* Memory profiling / allocator regression checks: Enable `memory-tracking` (optionally keep `stats`).

### Quick usage examples
Group by on a gzipped file (decompression auto‑handled):
```
cargo run --release -- -f data/log.gz -k 2,5 -s 9 --csv_output
```

Same command without ticker noise:
```
cargo run --release --no-default-features --features core-io -- -f data/log.gz -k 2,5 -s 9 --csv_output
```

Enable memory instrumentation for a focused run:
```
cargo run --release --features "memory-tracking" -- -f data/log.gz -k 2,5 -s 9 --csv_output
```

> Note: Feature selection is compile‑time; recompile when switching.

It does this job very fast by "slicing" blocks of data on line boundary points and forwarding those line-even blocks to multiple parser threads.
There are also multiple IO threads when list of files are provided as a data source.

- Fast - processing at 500MB/s to 2GB/s is not uncommon on fast multicore machines
- Do group-bys with sums, counts, count distincts, avg, min, and max
- Can process [CSV](https://crates.io/crates/csv) files OR text using [regular](https://www.pcre.org/current/doc/html/pcre2syntax.html) [expressions](https://crates.io/crates/pcre2)
- Process files or stdin as a data source:
  - csv files
  - text/log handled via regex mode where sub groups map to field positions
  - files are decompressed (like .zst, .gz, .xz, etc) on the fly
  - recursive [--walk](https://github.com/BurntSushi/ripgrep/tree/master/ignore) directory trees and filter for only the files you want
- Filenames (-p) can be filtered/parsed with regular expressions where sub-groups become fields.
- Delimited output or "aligned" table output



## HOW-TO

You identify fields as column numbers.  
These will either be part of the "key" or group-by, an aggregate (avg or sum), or count distinct.  
You may use none-to-many fields for each kind of field including the key field.
Use -A option to give these numbered fields "names" used in the output.

### Selecting Fields by Header Name (KEYS & AGGREGATES)  
Name-based column selection has been extended beyond keys. You can now use CSV header names anywhere you would normally provide 1-based numeric field positions for these options:

`-k` (keys), `-s` (sum), `-a` (avg), `-u` (distinct count), `-D` (write_distros), `-n` (min nums), `-x` (max nums), `-N` (min strings), `-X` (max strings)

All of these accept a comma‑separated list of either numbers, names, or a mix. Each token is interpreted as:
* If it parses as an integer > 0: treated as (index - 1) internally.
* Otherwise: treated as a header name to resolve.

Resolution occurs in a single pre-scan of the header (stdin buffered or each file's first line) before worker threads start. Multi‑file mode enforces identical headers.

Example (stdin, mixed key + aggregates by name):
```
printf 'digit,i,j,dvalue\n0,1,10,0\n1,2,20,2\n' | \
    gb -k digit -s dvalue -a digit -u i -n dvalue -x dvalue --skip_header --csv_output
```
Sample header line produced (indices become 1-based in output; names included when known):
```
k:1:digit,count,sum:4:dvalue,avg:1:digit,cnt_uniq:2:i,min:4:dvalue,max:4:dvalue
```

Another example mixing numeric & names:
```
gb -f events.csv -k 1,video_id -s views,total_duration --skip_header --csv_output
```
If the header starts with `date,video_id,views,total_duration,...` the keys resolve to indices 1 & 2; sums to indices 3 & 4. Duplicate or overlapping specifications are de‑duplicated after resolution.

Requirements & Notes:
* Always add `--skip_header` when using any names so the first line is treated as a header record (the internal CSV reader is switched to `has_headers(false)` after extraction so data lines are not skipped).
* Mixing numeric and name tokens is supported across all listed options.
* Regex mode (`-r`) still only supports numeric indices (capture groups) for now; name tokens will be rejected.
* Name collisions: the first occurrence wins; additional identical header names (rare / malformed CSV) map to the first index.
* Aliasing precedence for output labels: header name > explicit `-A` alias > bare numeric index.
* Output header format: each selected field or aggregate column label uses the pattern `<kind>:<1-based-index>[:<header-name>]` where `[:<header-name>]` is included only if a name was resolved.
* Distribution fields specified with `-D` must also appear (by index or name) in the `-u` unique list; this is validated after resolution when names are used.

Errors:
```
Missing header name(s) for -k: some_missing_column
Missing header name(s) for -s: duration_ms
write_distro specifies field 7 that is not a subset of the unique_keys
```

Performance Impact:
The header pre-scan cost is negligible relative to parallel parsing (single read of the first line per file or buffering all stdin once).

Limitations / Future Work:
* Regex mode name support (mapping capture groups to symbolic names) is not yet implemented.
* Option to suppress numeric indices in labels when a name exists (e.g. emit `k:digit` instead of `k:1:digit`) may be added—current tests rely on the numeric form.
* Potential expansion to allow custom output label templates.

Tip: If you want stable scripts across schema changes, prefer names; they fail fast if a required column is removed or renamed.



### Summaries on a csv file:

Using some airline flight data taken from:
http://stat-computing.org/dataexpo/2009/2008.csv.bz2

>*Note that this data is truncated a bit here and reformated from this csv to make it readable.*
```
  1     2        3           4         5         6          7          8           9

Year  Month  DayofMonth  DayOfWeek  DepTime  CRSDepTime  ArrTime  CRSArrTime  UniqueCarrier  ...
2008  1      3           4          2003     1955        2211     2225        WN             ...
2008  1      3           4          754      735         1002     1000        WN             ...
2008  1      3           4          628      620         804      750         WN             ...
2008  1      3           4          926      930         1054     1100        WN             ...
2008  1      3           4          1829     1755        1959     1925        WN             ...
2008  1      3           4          1940     1915        2121     2110        WN             ...
2008  1      3           4          1937     1830        2037     1940        WN             ...
2008  1      3           4          1039     1040        1132     1150        WN             ...
2008  1      3           4          617      615         652      650         WN             ...
....
```
Running the command:
```
gb -f 2008.csv -k 2,9 -s 14 --skip_header -c | head -10
```

How this command corresponds to the SQL:
```
select Month, UniqueCarrier, count(*), sum(AirTime) from csv group by Month, UniqueCarrier
       ^              ^                     ^
       |              |                     |
   -k  2,             9                -s   14
```

Here's a partial output:
``` 
 k:2 | k:9 | count  | s:14    | a:14
-----+-----+--------+---------+--------------------
 1   | 9E  | 22840  | 1539652 | 71.22412915760744
 1   | AA  | 52390  | 7317245 | 144.5524496246543
 1   | AQ  | 4026   | 247200  | 61.830915457728864
 1   | AS  | 12724  | 1587637 | 129.23378103378104
 1   | B6  | 16441  | 2477670 | 152.93315227455096
 1   | CO  | 25168  | 3878167 | 155.51858683883387
 1   | DL  | 38241  | 4812768 | 130.22967853663818
 1   | EV  | 23106  | 1584233 | 72.21739526826822
....
 ```

Another example that determines number of airplanes used and time spent in the air by that carrier.

```
select Carrier, count(*), sum(AirTime), count(distinct TailNum), sum(AirTime) average(AirTime) from csv 
group by Carrier
```
The following command emulates this:
```
gb -f ~/dl/2008.csv -k 9 -s 14 -u 11 -s 14 -a 14 --skip_header
```

Output:
> *Note that the output order of the columns does not correspond to the order of the field options.  It is fixed to keys, count, sums, avgs, and then uniques.*
```
 k:9 | count   | s:14      | s:14      | a:14               | u:11
-----|---------|-----------|-----------|--------------------|------
 9E  | 262109  | 18080077  | 18080077  | 71.11840692300127  | 162
 AA  | 604655  | 82989937  | 82989937  | 141.8001178963196  | 656
 AQ  | 7797    | 479581    | 479581    | 61.889405084527034 | 21
 AS  | 151045  | 19569900  | 19569900  | 131.83977040764768 | 126
 B6  | 196018  | 28849406  | 28849406  | 150.22524356777978 | 154
 CO  | 298342  | 45784515  | 45784515  | 155.86589297447088 | 378
 ...
 ```

## Summaries on arbitrary text
#### Using regular expression sub groups as data fields.
Alternatively, you can use a regular expression (see -r option) against the lines where the sub groups captured become fields and these field indices correspond with the subgroup.  This is useful for data that is not as organized like a csv such as logs files etc.

This example will peal off the date from mayapp log files and summarize the ERROR based on the first 5 to 40 characters of that line.

This is example of using the file path as part of the reporting.  
```
 gb --walk /some/log/directory -p 'myapp.*(2019-\d\d-\d\d).log' -r '.*ERROR(.{5,40}).*' -k 1,2
```
Here the subgroups of 1 and 2 are used to create a composite key of the date from the log file name, and the bit of text after the ERROR string in the log file.

## Test RE Mode
If you want to test how how a line of text and your regular expression interact use the options -R "some regular expression" and the -L "line of text" to get the sub groups gb will find.


## Help  `gb -h`
```
csv-groupby ver: 0.8.2  rev: 194704d  date: 2020-08-29
Execute a sql-like group-by on arbitrary text or csv files. Field indices start at 1.

Note that -l, -f, and -i define where data comes from.  If none of these options is given then it default to reading
stdin.

USAGE:
    gb [FLAGS] [OPTIONS]

FLAGS:
    -c, --csv_output                   Write delimited output
    -v                                 Verbosity - use more than one v for greater detail
        --skip_header                  Skip the first (header) line
        --no_record_count              Do not write records
        --noop_proc                    Do no real work - no parsing - for testing
    -i                                 Read a list of files to parse from stdin
        --stats                        Write stats after processing
        --no_output                    do not write summary output
        --recycle_io_blocks_disable    disable reusing memory io blocks
        --disable_key_sort             disables the key sort
    -E, --print_examples               Prints example usage scenarious - extra help
    -h, --help                         Prints help information
    -V, --version                      Prints version information

OPTIONS:
    -R, --test_re <testre>                            One-off test of a regex
    -L, --test_line <testline>...                     Line(s) of text to test with -R option instead of stdin
    -k, --key_fields <keyfield>...                    Fields that will act as group by keys
    -u, --unique_values <uniquefield>...              Fields to count distinct
    -D, --write_distros <writedistros>...             write unique value distro with -u option
        --write_distros_upper <writedistrosupper>     number highest value x count [default: 5]
        --write_distros_bottom <writedistrobottom>    number lowest value x count [default: 2]
    -s, --sum_values <sumfield>...                    Sum fields as float64s
    -a, --avg_values <avg_fields>...                  Average fields
    -x, --max_nums <max_num_fields>...                Max fields as float64s
    -n, --min_nums <min_num_fields>...                Min fieldss as float64
    -X, --max_strings <max_str_fields>...             Max fields as string
    -N, --min_strings <min_str_fields>...             Min fields as string
    -A, --field-aliases <field_aliases>...            Alias the field positions to meaningful names
    -r, --regex <re-str>...                           Regex mode regular expression to parse fields
    -p, --path_re <re-path>                           Match path on files and get fields from sub groups
        --re_line_contains <re-line-contains>         Grep lines that must contain a string
    -d, --input_delimiter <delimiter>                 Delimiter if in csv mode [default: ,]
    -q, --quote <quote>                               csv quote character
    -e, --escape <escape>                             csv escape character
    -C, --comment <comment>                           csv mode comment character
    -o, --output_delimiter <outputdelimiter>          Output delimiter for written summaries [default: ,]
        --empty_string <empty>                        Empty string substitution [default: ]
    -t, --parse_threads <parse-threads>               Number of parser threads [default: 12]
    -I, --io_threads <io-threads>                     Number of IO threads [default: 6]
        --queue_size <thread-qsize>                   Queue length of blocks between threads [default: 48]
        --path_qsize <path-qsize>                     Queue length of paths to IO slicer threads [default: 0]
        --io_block_size <io-block-size>               IO block size - 0 use default [default: 0]
        --q_block_size <q-block-size>                 Block size between IO thread and worker [default: 256K]
    -l <file_list>                                    A file containing a list of input files
    -f <file>...                                      List of input files
    -w, --walk <walk>                                 recursively walk a tree of files to parse
        --null_write <nullstring>                     String to use for NULL fields [default: NULL]
```
            
TODO/ideas:  

```

--where 1:<re>
--where_not 1:<re>
--tail &| --head
--count_ge <num>
--count_le

~--sort_count_desc
--sort_count_asc

```


- Fix -i to also support -p option
    > fixed
- More better readme - sometimes more is not better
- More diverse unit testing
- Oh musl, where for art thou musl?  Why your alloc so bad....
- more aggregates: min, max, empty_count, number_count, zero_count
  - avg and sum done
  - max and min done for string and numbers
- faster sort on mixed key fields before output
- faster output for csv output mode
   > faster, but not as fast as possible
- do more work to multi-line re mode - not sure how it should really work yet
    > This does function and I have used it on well formed xml, but not sure it will be a thing or not.
- flat mode - no summary but just write X fields to the output as found - a kind s/()..()..()/$1,$2,$3,.../;
- use RE find/search for matches instead of line bound interface?

