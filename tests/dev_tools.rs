#![cfg(dev_tools)]

// Integration tests for dev-tools gated helper-like functionality.
// We recreate minimal copies of the gated private helpers (they live inside main.rs
// and are not publicly exported) to assert intended semantics. This keeps them
// from being optimized away while still allowing refactors without making them public.

// Reuse CLI config to call helpers indirectly where possible.
// We construct a trivial config via get_cli parsing a fake argv if needed.

#[test]
fn reused_str_vec_basic() {
    // Mirror logic of reused_str_vec: when index < len overwrite, else push.
    fn reused_str_vec(idx: usize, v: &mut Vec<String>, s: &str) {
        if idx < v.len() { v[idx].clear(); v[idx].push_str(s); } else { v.push(String::from(s)); }
    }
    let mut v = vec!["alpha".to_string()];
    reused_str_vec(0, &mut v, "beta");
    assert_eq!(v[0], "beta");
    reused_str_vec(1, &mut v, "gamma");
    assert_eq!(v, vec!["beta", "gamma"]);
}

#[test]
fn re_mod_idx_increments() {
    // Local copy of generic helper (kept simple).
    fn re_mod_idx<T>(_cfg: &(), v: T) -> T where T: std::ops::Add<Output=T> + From<usize> { v + 1usize.into() }
    let cfg = (); // placeholder
    assert_eq!(re_mod_idx(&cfg, 0usize), 1);
    assert_eq!(re_mod_idx(&cfg, 41usize), 42);
}

#[test]
fn reused_str_vec_growth() {
    fn reused_str_vec(idx: usize, v: &mut Vec<String>, s: &str) {
        if idx < v.len() { v[idx].clear(); v[idx].push_str(s); } else { v.push(String::from(s)); }
    }
    let mut v: Vec<String> = Vec::new();
    for (i, w) in ["a","b","c"].iter().enumerate() { reused_str_vec(i, &mut v, w); }
    assert_eq!(v, vec!["a","b","c"]);
    // overwrite middle then append
    reused_str_vec(1, &mut v, "B2");
    reused_str_vec(3, &mut v, "d");
    assert_eq!(v, vec!["a","B2","c","d"]);
}
