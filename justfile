# deltaR Development Commands
# Run `just --list` to see all available commands

# Default recipe: list all commands
default:
    @just --list

# Set environment variables for faster R package development
export NOT_CRAN := "true"

# Run all tests
test:
    Rscript -e "devtools::test()"

# Run tests with a filter pattern
test-filter pattern:
    Rscript -e "devtools::test(filter = '{{pattern}}')"

# Run R CMD check
check:
    Rscript -e "devtools::check()"
# Build and install the package locally
install:
    Rscript -e "devtools::install()"

# Document the package (regenerate Rd files and NAMESPACE)
document:
    Rscript -e "devtools::document()"

# Regenerate extendr wrappers from Rust code
extendr:
    Rscript -e "rextendr::document()"

# Build vignettes
vignettes:
    Rscript -e "devtools::build_vignettes()"

# Build the package tarball
build:
    Rscript -e "devtools::build()"

# Load the package for interactive development
load:
    Rscript -e "devtools::load_all()"

# Run a quick development cycle: document + test
dev: document test

# Full check cycle: document + check
full-check: document check

# Clean Rust build artifacts
clean-rust:
    cd src/rust && cargo clean

# Clean all build artifacts
clean: clean-rust
    rm -rf src/*.o src/*.so src/*.dll
    rm -rf man/*.Rd
    rm -rf Meta

# Build Rust library in release mode
build-rust:
    cd src/rust && cargo build --release

# Build Rust library in debug mode
build-rust-debug:
    cd src/rust && cargo build

# Run Rust tests
test-rust:
    cd src/rust && cargo test

# Check Rust code (faster than full build)
check-rust:
    cd src/rust && cargo check

# Format Rust code
fmt-rust:
    cd src/rust && cargo fmt

# Lint Rust code with clippy
clippy:
    cd src/rust && cargo clippy

# Build pkgdown site
site:
    Rscript -e "pkgdown::build_site()"

# Preview pkgdown site locally
site-preview:
    Rscript -e "pkgdown::preview_site()"

# Run spell check on documentation
spell-check:
    Rscript -e "devtools::spell_check()"

# Update Rust dependencies
update-rust:
    cd src/rust && cargo update

# Show outdated Rust dependencies
outdated-rust:
    cd src/rust && cargo outdated

# Generate code coverage report
coverage:
    Rscript -e "covr::package_coverage()"

# Open package documentation in browser
docs:
    Rscript -e "devtools::dev_help('write_deltalake')"

# Print R and package version info
info:
    @echo "R version:"
    @Rscript -e "R.version.string"
    @echo "\nRust version:"
    @rustc --version
    @cargo --version
    @echo "\nPackage dependencies:"
    @Rscript -e "devtools::session_info()"
