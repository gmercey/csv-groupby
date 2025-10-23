fn main() {
    if let Some(rev) = git_revision_hash() {
        println!("cargo:rustc-env=BUILD_GIT_HASH={rev}");
    } else {
        println!("cargo:rustc-env=BUILD_GIT_HASH=GIT FAILED");
    }
    // Declare custom cfg so rustc's check-cfg lint knows about it (used to gate dev helpers)
    println!("cargo:rustc-check-cfg=cfg(dev_tools)");
}

fn git_revision_hash() -> Option<String> {
    let result = std::process::Command::new("git")
        .args(["rev-parse", "--short=10", "HEAD"])
        .output();
    result.ok().and_then(|output| {
        let v = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if v.is_empty() {
            None
        } else {
            Some(v)
        }
    })
}

