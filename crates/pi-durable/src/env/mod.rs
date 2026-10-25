//! Portable environment capabilities. The filesystem operations used by durable
//! storage share the same authority and error model as later tool environments.
pub mod decode;
pub mod filesystem;
pub mod line_scan;
pub mod native;
pub mod native_shell;
pub mod native_watch;
pub mod readers;
pub mod shell;
pub use filesystem::*;
pub use line_scan::{LineScan, LineScanner};
pub use native::{NativeExecutionEnv, NativeFileSystem};
pub use native_shell::{
    NativeShellOptions, ShellHost, ShellPlatform, ShellSpillOpener, ShellSpillWriter, ShellTempFile,
};
pub use native_watch::{
    NativeFileWatcher, NativeWatchCallback, NativeWatchEvent, NativeWatchHandle, NativeWatchOpener,
    NativeWatchOptions,
};
pub use shell::*;
