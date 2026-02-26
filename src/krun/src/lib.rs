//! krun_api - Native Rust API for libkrun microVMs.
//!
//! This crate provides a builder-pattern API for creating and running microVMs
//! using libkrun's VMM infrastructure.
//!
//! # Example
//!
//! ```rust,no_run
//! use krun_api::{VmBuilder, Result};
//!
//! fn main() -> Result<()> {
//!     let exit_code = VmBuilder::new()
//!         .machine(|m| m.vcpus(4).memory_mib(2048))
//!         .fs(|fs| fs.root("/path/to/rootfs"))
//!         .exec(|e| e.path("/bin/myapp").args(["--flag"]).env("HOME", "/root"))
//!         .build()?
//!         .run()?;
//!
//!     println!("VM exited with code: {}", exit_code);
//!     Ok(())
//! }
//! ```

//--------------------------------------------------------------------------------------------------
// Modules
//--------------------------------------------------------------------------------------------------

pub mod api;
pub mod backends;

//--------------------------------------------------------------------------------------------------
// Re-Exports
//--------------------------------------------------------------------------------------------------

pub use api::builder::VmBuilder;
pub use api::builders::{ConsoleBuilder, ExecBuilder, FsBuilder, KernelBuilder, MachineBuilder};
#[cfg(feature = "blk")]
pub use api::builders::DiskBuilder;
#[cfg(feature = "net")]
pub use api::builders::NetBuilder;
pub use api::error::{BuildError, ConfigError, Error, Result, RuntimeError};
pub use api::vm::Vm;

#[cfg(not(any(feature = "tee", feature = "aws-nitro")))]
pub use backends::fs::DynFileSystem;

#[cfg(feature = "net")]
pub use backends::net::NetBackend;
