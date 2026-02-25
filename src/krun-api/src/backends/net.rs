//! Network backend support.
//!
//! This module re-exports the `NetBackend` trait from the devices crate,
//! allowing users to implement custom network backends for their VMs.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::os::fd::RawFd;
//! use krun::backends::net::{NetBackend, ReadError, WriteError};
//!
//! struct MyNetBackend {
//!     // ... your implementation
//! }
//!
//! impl NetBackend for MyNetBackend {
//!     fn read_frame(&mut self, buf: &mut [u8]) -> Result<usize, ReadError> {
//!         // Read an ethernet frame from the backend
//!         todo!()
//!     }
//!
//!     fn write_frame(&mut self, hdr_len: usize, buf: &mut [u8]) -> Result<(), WriteError> {
//!         // Write an ethernet frame to the backend
//!         todo!()
//!     }
//!
//!     fn has_unfinished_write(&self) -> bool {
//!         false
//!     }
//!
//!     fn try_finish_write(&mut self, hdr_len: usize, buf: &[u8]) -> Result<(), WriteError> {
//!         Ok(())
//!     }
//!
//!     fn raw_socket_fd(&self) -> RawFd {
//!         // Return the raw fd for epoll registration
//!         todo!()
//!     }
//! }
//! ```

//--------------------------------------------------------------------------------------------------
// Re-Exports
//--------------------------------------------------------------------------------------------------

#[cfg(feature = "net")]
pub use devices::virtio::net::backend::{ConnectError, NetBackend, ReadError, WriteError};
