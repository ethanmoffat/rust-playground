use std::process::Command;

#[test]
#[ignore = "multiprocess protocol qualification; run explicitly"]
fn quick_protocol_spike() {
    if !cfg!(target_os = "linux") {
        return;
    }

    let status = Command::new(env!("CARGO_BIN_EXE_arrowcol-driver"))
        .args(["--profile", "quick"])
        .status()
        .unwrap_or_else(|error| panic!("failed to run protocol driver: {error}"));
    assert!(status.success(), "protocol driver exited with {status}");
}
