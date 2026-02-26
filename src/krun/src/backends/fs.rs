//! Filesystem backend support.
//!
//! This module provides the `DynFileSystem` trait, an object-safe version of
//! the `FileSystem` trait from the devices crate. This allows custom filesystem
//! implementations to be used with libkrun's virtio-fs device.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::ffi::CStr;
//! use std::io;
//! use std::time::Duration;
//! use krun::backends::fs::{DynFileSystem, Context, Entry, FsOptions};
//!
//! struct MyFileSystem {
//!     // ... your implementation
//! }
//!
//! impl DynFileSystem for MyFileSystem {
//!     fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
//!         Ok(FsOptions::empty())
//!     }
//!
//!     fn lookup(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<Entry> {
//!         // Implement file lookup
//!         todo!()
//!     }
//!
//!     // ... implement other methods as needed
//! }
//! ```

#[cfg(target_os = "macos")]
use crossbeam_channel::Sender;
#[cfg(target_os = "macos")]
use utils::worker_message::WorkerMessage;

use std::ffi::CStr;
use std::io;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::time::Duration;

//--------------------------------------------------------------------------------------------------
// Re-Exports
//--------------------------------------------------------------------------------------------------

pub use devices::virtio::bindings::{stat64, statvfs64};
pub use devices::virtio::fs::filesystem::{
    Context, DirEntry, Entry, Extensions, FsOptions, GetxattrReply, ListxattrReply, OpenOptions,
    RemovemappingOne, SecContext, SetattrValid, ZeroCopyReader, ZeroCopyWriter,
};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

/// Linux ENOSYS error code.
const LINUX_ENOSYS: i32 = 38;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Object-safe filesystem trait for dynamic dispatch.
///
/// This trait mirrors the `FileSystem` trait from the devices crate but uses
/// `u64` directly for `Inode` and `Handle` instead of associated types, enabling
/// object safety.
///
/// Most methods have default implementations that return `ENOSYS`, allowing
/// implementations to only override the methods they need.
#[allow(unused_variables)]
pub trait DynFileSystem: Send + Sync {
    /// Initialize the file system.
    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        Ok(FsOptions::empty())
    }

    /// Clean up the file system.
    fn destroy(&self) {}

    /// Look up a directory entry by name and get its attributes.
    fn lookup(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Forget about an inode.
    fn forget(&self, ctx: Context, inode: u64, count: u64) {}

    /// Forget about multiple inodes.
    fn batch_forget(&self, ctx: Context, requests: Vec<(u64, u64)>) {
        for (inode, count) in requests {
            self.forget(ctx, inode, count);
        }
    }

    /// Get attributes for a file / directory.
    fn getattr(
        &self,
        ctx: Context,
        inode: u64,
        handle: Option<u64>,
    ) -> io::Result<(stat64, Duration)> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Set attributes for a file / directory.
    fn setattr(
        &self,
        ctx: Context,
        inode: u64,
        attr: stat64,
        handle: Option<u64>,
        valid: SetattrValid,
    ) -> io::Result<(stat64, Duration)> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Read a symbolic link.
    fn readlink(&self, ctx: Context, inode: u64) -> io::Result<Vec<u8>> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Create a symbolic link.
    fn symlink(
        &self,
        ctx: Context,
        linkname: &CStr,
        parent: u64,
        name: &CStr,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Create a file node.
    fn mknod(
        &self,
        ctx: Context,
        inode: u64,
        name: &CStr,
        mode: u32,
        rdev: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Create a directory.
    fn mkdir(
        &self,
        ctx: Context,
        parent: u64,
        name: &CStr,
        mode: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Remove a file.
    fn unlink(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Remove a directory.
    fn rmdir(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Rename a file / directory.
    fn rename(
        &self,
        ctx: Context,
        olddir: u64,
        oldname: &CStr,
        newdir: u64,
        newname: &CStr,
        flags: u32,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Create a hard link.
    fn link(&self, ctx: Context, inode: u64, newparent: u64, newname: &CStr) -> io::Result<Entry> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Open a file.
    fn open(&self, ctx: Context, inode: u64, flags: u32) -> io::Result<(Option<u64>, OpenOptions)> {
        Ok((None, OpenOptions::empty()))
    }

    /// Create and open a file.
    fn create(
        &self,
        ctx: Context,
        parent: u64,
        name: &CStr,
        mode: u32,
        flags: u32,
        umask: u32,
        extensions: Extensions,
    ) -> io::Result<(Entry, Option<u64>, OpenOptions)> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Read data from a file.
    fn read(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        w: &mut dyn ZeroCopyWriter,
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        flags: u32,
    ) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Write data to a file.
    fn write(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        r: &mut dyn ZeroCopyReader,
        size: u32,
        offset: u64,
        lock_owner: Option<u64>,
        delayed_write: bool,
        kill_priv: bool,
        flags: u32,
    ) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Flush the contents of a file.
    fn flush(&self, ctx: Context, inode: u64, handle: u64, lock_owner: u64) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Synchronize file contents.
    fn fsync(&self, ctx: Context, inode: u64, datasync: bool, handle: u64) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Allocate requested space for file data.
    fn fallocate(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        mode: u32,
        offset: u64,
        length: u64,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Release an open file.
    fn release(
        &self,
        ctx: Context,
        inode: u64,
        flags: u32,
        handle: u64,
        flush: bool,
        flock_release: bool,
        lock_owner: Option<u64>,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Get information about the file system.
    fn statfs(&self, ctx: Context, inode: u64) -> io::Result<statvfs64> {
        // Safe because we are zero-initializing a struct with only POD fields.
        let mut st: statvfs64 = unsafe { std::mem::zeroed() };
        st.f_namemax = 255;
        st.f_bsize = 512;
        Ok(st)
    }

    /// Set an extended attribute.
    fn setxattr(
        &self,
        ctx: Context,
        inode: u64,
        name: &CStr,
        value: &[u8],
        flags: u32,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Get an extended attribute.
    fn getxattr(
        &self,
        ctx: Context,
        inode: u64,
        name: &CStr,
        size: u32,
    ) -> io::Result<GetxattrReply> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// List extended attribute names.
    fn listxattr(&self, ctx: Context, inode: u64, size: u32) -> io::Result<ListxattrReply> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Remove an extended attribute.
    fn removexattr(&self, ctx: Context, inode: u64, name: &CStr) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Open a directory for reading.
    fn opendir(
        &self,
        ctx: Context,
        inode: u64,
        flags: u32,
    ) -> io::Result<(Option<u64>, OpenOptions)> {
        Ok((None, OpenOptions::empty()))
    }

    /// Read a directory.
    ///
    /// Returns a vector of directory entries. Unlike the original FileSystem trait
    /// which uses a callback, this returns entries directly for object safety.
    fn readdir(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        size: u32,
        offset: u64,
    ) -> io::Result<Vec<DirEntry<'static>>> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Read a directory with entry attributes.
    ///
    /// Returns a vector of (DirEntry, Entry) pairs. Unlike the original FileSystem
    /// trait which uses a callback, this returns entries directly for object safety.
    fn readdirplus(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        size: u32,
        offset: u64,
    ) -> io::Result<Vec<(DirEntry<'static>, Entry)>> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Synchronize the contents of a directory.
    fn fsyncdir(&self, ctx: Context, inode: u64, datasync: bool, handle: u64) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Release an open directory.
    fn releasedir(&self, ctx: Context, inode: u64, flags: u32, handle: u64) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Check file access permissions.
    fn access(&self, ctx: Context, inode: u64, mask: u32) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Reposition read/write file offset.
    fn lseek(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        offset: u64,
        whence: u32,
    ) -> io::Result<u64> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Copy a range of data from one file to another.
    fn copyfilerange(
        &self,
        ctx: Context,
        inode_in: u64,
        handle_in: u64,
        offset_in: u64,
        inode_out: u64,
        handle_out: u64,
        offset_out: u64,
        len: u64,
        flags: u64,
    ) -> io::Result<usize> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Setup a mapping for DAX.
    fn setupmapping(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        foffset: u64,
        len: u64,
        flags: u64,
        moffset: u64,
        host_shm_base: u64,
        shm_size: u64,
        #[cfg(target_os = "macos")] map_sender: &Option<Sender<WorkerMessage>>,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    /// Remove a DAX mapping.
    fn removemapping(
        &self,
        ctx: Context,
        requests: Vec<RemovemappingOne>,
        host_shm_base: u64,
        shm_size: u64,
        #[cfg(target_os = "macos")] map_sender: &Option<Sender<WorkerMessage>>,
    ) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }

    /// Perform an ioctl on a file.
    fn ioctl(
        &self,
        ctx: Context,
        inode: u64,
        handle: u64,
        flags: u32,
        cmd: u32,
        arg: u64,
        in_size: u32,
        out_size: u32,
        exit_code: &Arc<AtomicI32>,
    ) -> io::Result<Vec<u8>> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Get file lock (not yet supported).
    fn getlk(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Set file lock (not yet supported).
    fn setlk(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Set file lock and wait (not yet supported).
    fn setlkw(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Map block index to block number (not yet supported).
    fn bmap(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Poll for events (not yet supported).
    fn poll(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }

    /// Reply to a notification (not yet supported).
    fn notify_reply(&self) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(LINUX_ENOSYS))
    }
}
