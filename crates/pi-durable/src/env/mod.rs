//! Portable environment capabilities. The filesystem operations used by durable
//! storage share the same authority and error model as later tool environments.
pub mod filesystem;
pub mod native;
pub mod shell;
pub use filesystem::*;
pub use native::NativeFileSystem;
pub use shell::*;
