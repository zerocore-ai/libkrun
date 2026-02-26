//! Native Rust API for libkrun.
//!
//! This module provides a builder-pattern API for creating and running microVMs
//! using nested builders for organized configuration.
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

pub mod builder;
pub mod builders;
pub mod error;
pub mod vm;

//--------------------------------------------------------------------------------------------------
// Re-Exports
//--------------------------------------------------------------------------------------------------

pub use builder::VmBuilder;
#[cfg(feature = "blk")]
pub use builders::DiskBuilder;
#[cfg(feature = "net")]
pub use builders::NetBuilder;
pub use builders::{ConsoleBuilder, ExecBuilder, FsBuilder, KernelBuilder, MachineBuilder};
pub use error::{BuildError, ConfigError, Error, Result, RuntimeError};
pub use vm::Vm;
