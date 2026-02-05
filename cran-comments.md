## Test environments
* ubuntu-latest (on GitHub Actions), R-devel, rustc 1.88.0
* ubuntu-latest (on GitHub Actions), R-release, rustc 1.88.0
* ubuntu-latest (on GitHub Actions), R-oldrel-1, rustc 1.88.0
* macos-latest (on GitHub Actions), R-release, rustc 1.88.0
* windows-latest (on GitHub Actions), R-release, rustc 1.88.0

## R CMD check results

0 errors | 0 warnings | 0 notes

## Rust Compatibility
This package has been extensively tested with Rust 1.88.0. The `SystemRequirements` field in `DESCRIPTION` specifies `rustc >= 1.88` to ensure compatibility with the underlying `delta-rs` components. Continuous Integration via GitHub Actions confirms successful builds and unit tests across Linux, macOS, and Windows using this Rust version.
