# Note: Any variables prefixed with `.` are used for text
# replacement in the Makevars.in and Makevars.win.in

# check the packages MSRV first
source("tools/msrv.R")

# check if we are on Windows
is_windows <- .Platform[["OS.type"]] == "windows"

# Generate a short target directory for Windows to avoid MAX_PATH issues
# aws-lc-sys builds fail when paths exceed ~250 characters
# IMPORTANT: Both CARGO_HOME and TARGET_DIR must be in the same short path
# so that relative paths in aws-lc-sys CMake builds resolve correctly
if (is_windows) {
  # For CI builds (NOT_CRAN set), we can use a fixed short path

  # For CRAN builds, we use tempdir but write the path to a file so that

  # subsequent R sessions (configure vs build) use the same directory

  env_not_cran_check <- Sys.getenv("NOT_CRAN")

  if (env_not_cran_check != "") {
    # CI build - use fixed short path (no CRAN policy concerns)
    short_base <- "C:/tmp/dR"
  } else {
    # CRAN build - use tempdir but persist the path choice
    # Create a marker file in the source directory to ensure consistency
    marker_file <- "src/.short_build_path"
    if (file.exists(marker_file)) {
      short_base <- readLines(marker_file, n = 1)
    } else {
      short_base <- gsub("\\\\", "/", file.path(tempdir(), "dR"))
      writeLines(short_base, marker_file)
    }
  }

  # Normalize to forward slashes for Make compatibility
  .short_base <- short_base
  .target_dir <- paste0(short_base, "/target")
  .cargo_home <- paste0(short_base, "/.cargo")
  message("Using short build path: ", short_base)
} else {
  .short_base <- "$(CURDIR)"
  .target_dir <- "./rust/target"
  .cargo_home <- "$(CURDIR)/.cargo"
}

# check DEBUG and NOT_CRAN environment variables
env_debug <- Sys.getenv("DEBUG")
env_not_cran <- Sys.getenv("NOT_CRAN")

# check if the vendored zip file exists
vendor_exists <- file.exists("src/rust/vendor.tar.xz")

is_not_cran <- env_not_cran != ""
is_debug <- env_debug != ""

if (is_debug) {
  # if we have DEBUG then we set not cran to true
  # CRAN is always release build
  is_not_cran <- TRUE
  message("Creating DEBUG build.")
}

if (!is_not_cran) {
  message("Building for CRAN.")
}

# we set cran flags only if NOT_CRAN is empty and if
# the vendored crates are present.
.cran_flags <- ifelse(
  !is_not_cran && vendor_exists,
  "-j 2 --offline",
  ""
)

# when DEBUG env var is present we use `--debug` build
.profile <- ifelse(is_debug, "", "--release")
.clean_targets <- ifelse(is_debug, "", "$(TARGET_DIR)")

# We specify this target when building for webR
webr_target <- "wasm32-unknown-emscripten"

# here we check if the platform we are building for is webr
is_wasm <- identical(R.version$platform, webr_target)

# print to terminal to inform we are building for webr
if (is_wasm) {
  message("Building for WebR")
}

# we check if we are making a debug build or not
# if so, the LIBDIR environment variable becomes:
# LIBDIR = $(TARGET_DIR)/{wasm32-unknown-emscripten}/debug
# this will be used to fill out the LIBDIR env var for Makevars.in
target_libpath <- if (is_wasm) "wasm32-unknown-emscripten" else NULL
cfg <- if (is_debug) "debug" else "release"

# used to replace @LIBDIR@
.libdir <- paste(c(target_libpath, cfg), collapse = "/")

# use this to replace @TARGET@
# we specify the target _only_ on webR
# there may be use cases later where this can be adapted or expanded
.target <- ifelse(is_wasm, paste0("--target=", webr_target), "")

# add panic exports only for WASM builds
.panic_exports <- ifelse(
  is_wasm,
  "CARGO_PROFILE_DEV_PANIC=\"abort\" CARGO_PROFILE_RELEASE_PANIC=\"abort\" ",
  ""
)

# read in the Makevars.in file checking

# if windows we replace in the Makevars.win.in
mv_fp <- ifelse(
  is_windows,
  "src/Makevars.win.in",
  "src/Makevars.in"
)

# set the output file
mv_ofp <- ifelse(
  is_windows,
  "src/Makevars.win",
  "src/Makevars"
)

# delete the existing Makevars{.win/.wasm}
if (file.exists(mv_ofp)) {
  message("Cleaning previous `", mv_ofp, "`.")
  invisible(file.remove(mv_ofp))
}

# read as a single string
mv_txt <- readLines(mv_fp)

# replace placeholder values
new_txt <- gsub("@CRAN_FLAGS@", .cran_flags, mv_txt) |>
  gsub("@PROFILE@", .profile, x = _) |>
  gsub("@CLEAN_TARGET@", .clean_targets, x = _) |>
  gsub("@LIBDIR@", .libdir, x = _) |>
  gsub("@TARGET@", .target, x = _) |>
  gsub("@PANIC_EXPORTS@", .panic_exports, x = _) |>
  gsub("@TARGET_DIR@", .target_dir, x = _) |>
  gsub("@CARGO_HOME@", .cargo_home, x = _) |>
  gsub("@SHORT_BASE@", .short_base, x = _)

message("Writing `", mv_ofp, "`.")
con <- file(mv_ofp, open = "wb")
writeLines(new_txt, con, sep = "\n")
close(con)

message("`tools/config.R` has finished.")
