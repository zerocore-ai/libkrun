//! Custom backend support for libkrun.
//!
//! This module provides traits and types for implementing custom backends
//! for libkrun's virtio devices.
//!
//! # Network Backends
//!
//! The `net` module re-exports the `NetBackend` trait which allows custom
//! network implementations. See [`net::NetBackend`] for details.
//!
//! # Filesystem Backends
//!
//! The `fs` module provides the `DynFileSystem` trait, an object-safe version
//! of the `FileSystem` trait. See [`fs::DynFileSystem`] for details.

//--------------------------------------------------------------------------------------------------
// Modules
//--------------------------------------------------------------------------------------------------

#[cfg(not(any(feature = "tee", feature = "nitro")))]
pub mod fs;

#[cfg(feature = "net")]
pub mod net;

//--------------------------------------------------------------------------------------------------
// Re-Exports
//--------------------------------------------------------------------------------------------------

#[cfg(not(any(feature = "tee", feature = "nitro")))]
pub use fs::DynFileSystem;

#[cfg(feature = "net")]
pub use net::NetBackend;
