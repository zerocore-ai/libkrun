# libkrun Rust API Design Plan

## Overview

This document outlines the design for a native Rust API for libkrun, replacing the current C FFI interface with an idiomatic Rust builder pattern.

## Goals

- Provide a safe, ergonomic Rust API
- Use builder pattern for VM configuration
- No global state (`CTX_MAP`)
- Feature-gated optional functionality
- Zero-cost abstractions where possible

## Architecture

```
src/libkrun/
├── lib.rs              # Public re-exports
├── builder.rs          # VmBuilder
├── vm.rs               # Vm struct and run logic
├── error.rs            # Error types
└── config/
    ├── mod.rs          # Config module exports
    ├── machine.rs      # MachineConfig
    ├── kernel.rs       # KernelConfig, KernelFormat
    ├── net.rs          # NetConfig, NetBackend
    ├── fs.rs           # FsConfig
    ├── block.rs        # BlockConfig, DiskFormat, SyncMode
    ├── console.rs      # ConsoleConfig, PortConfig
    ├── vsock.rs        # VsockConfig
    ├── gpu.rs          # GpuConfig, DisplayConfig
    ├── sound.rs        # SoundConfig
    └── exec.rs         # ExecConfig (workdir, args, env)
```

---

## Public API

### Entry Point

```rust
use libkrun::{Vm, VmBuilder, Error};

fn main() -> Result<(), Error> {
    let exit_code = VmBuilder::new()
        .machine(|m| m.vcpus(2).memory_mib(1024))
        .kernel(|k| k.path("/path/to/vmlinux").format(KernelFormat::Raw))
        .initramfs("/path/to/initramfs.cpio.gz")
        .mount(|fs| fs.tag("root").path("/"))
        .exec(|e| e.path("/bin/bash").args(["bash"]).env("HOME", "/root"))
        .build()?
        .run()?;

    std::process::exit(exit_code);
}
```

### VmBuilder

```rust
pub struct VmBuilder {
    config: VmConfig,
}

impl VmBuilder {
    pub fn new() -> Self;

    // Machine configuration
    pub fn machine(self, f: impl FnOnce(MachineBuilder) -> MachineBuilder) -> Self;

    // Kernel/boot configuration
    pub fn kernel(self, f: impl FnOnce(KernelBuilder) -> KernelBuilder) -> Self;
    pub fn initramfs(self, path: impl AsRef<Path>) -> Self;
    pub fn firmware(self, path: impl AsRef<Path>) -> Self;
    pub fn cmdline(self, cmdline: &str) -> Self;

    // Filesystem (virtio-fs)
    pub fn fs(self, f: impl FnOnce(FsBuilder) -> FsBuilder) -> Self;

    // Block devices (virtio-blk)
    #[cfg(feature = "blk")]
    pub fn disk(self, f: impl FnOnce(DiskBuilder) -> DiskBuilder) -> Self;
    #[cfg(feature = "blk")]
    pub fn root_disk(self, path: impl AsRef<Path>) -> Self;

    // Network (virtio-net)
    #[cfg(feature = "net")]
    pub fn net(self, f: impl FnOnce(NetBuilder) -> NetBuilder) -> Self;

    // Console
    pub fn console(self, f: impl FnOnce(ConsoleBuilder) -> ConsoleBuilder) -> Self;
    pub fn serial_console(self, input: RawFd, output: RawFd) -> Self;

    // GPU/Display
    #[cfg(feature = "gpu")]
    pub fn gpu(self, f: impl FnOnce(GpuBuilder) -> GpuBuilder) -> Self;
    #[cfg(feature = "gpu")]
    pub fn display(self, width: u32, height: u32) -> Self;

    // Sound
    #[cfg(feature = "snd")]
    pub fn sound(self, enabled: bool) -> Self;

    // Vsock / TSI
    pub fn vsock_port(self, guest_port: u32, host_fd: RawFd) -> Self;
    pub fn tsi_port_map(self, guest_port: u16, host_port: u16) -> Self;

    // Execution environment
    pub fn exec(self, f: impl FnOnce(ExecBuilder) -> ExecBuilder) -> Self;
    pub fn workdir(self, path: impl AsRef<Path>) -> Self;
    pub fn uid(self, uid: u32) -> Self;
    pub fn gid(self, gid: u32) -> Self;

    // Build
    pub fn build(self) -> Result<Vm, Error>;
}
```

### Vm

```rust
pub struct Vm {
    config: VmConfig,
}

impl Vm {
    /// Run the VM. Blocks until the guest exits.
    /// Returns the guest exit code.
    pub fn run(self) -> Result<i32, Error>;

    /// Get a shutdown event fd that can be used to signal VM shutdown.
    pub fn shutdown_fd(&self) -> Result<RawFd, Error>;
}
```

---

## Configuration Types

### MachineConfig

```rust
pub struct MachineBuilder { .. }

impl MachineBuilder {
    pub fn vcpus(self, count: u8) -> Self;           // 1-32
    pub fn memory_mib(self, mib: u32) -> Self;
    pub fn nested_virt(self, enabled: bool) -> Self;
}
```

**Maps to:** `VmConfig` in `src/vmm/src/vmm_config/machine_config.rs`

---

### KernelConfig

```rust
pub struct KernelBuilder { .. }

impl KernelBuilder {
    pub fn path(self, path: impl AsRef<Path>) -> Self;
    pub fn format(self, format: KernelFormat) -> Self;
    pub fn cmdline(self, cmdline: &str) -> Self;
}

pub enum KernelFormat {
    Raw,        // 0 - Uncompressed
    Elf,        // 1 - ELF binary
    PeGz,       // 2 - PE + GZIP
    ImageBz2,   // 3 - ARM64 Image + BZIP2
    ImageGz,    // 4 - ARM64 Image + GZIP
    ImageZstd,  // 5 - ARM64 Image + ZSTD
}
```

**Maps to:** `ExternalKernel`, `KernelFormat` in `src/vmm/src/vmm_config/external_kernel.rs`

---

### FsConfig (virtio-fs)

```rust
pub struct FsBuilder { .. }

impl FsBuilder {
    pub fn tag(self, tag: &str) -> Self;             // Guest mount tag
    pub fn path(self, path: impl AsRef<Path>) -> Self; // Host path
    pub fn shm_size(self, size: usize) -> Self;      // DAX window size
}
```

**Maps to:** `FsDeviceConfig` in `src/vmm/src/vmm_config/fs.rs`

---

### BlockConfig (virtio-blk)

```rust
#[cfg(feature = "blk")]
pub struct DiskBuilder { .. }

#[cfg(feature = "blk")]
impl DiskBuilder {
    pub fn id(self, id: &str) -> Self;
    pub fn path(self, path: impl AsRef<Path>) -> Self;
    pub fn format(self, format: DiskFormat) -> Self;
    pub fn read_only(self, read_only: bool) -> Self;
    pub fn direct_io(self, direct: bool) -> Self;
    pub fn sync_mode(self, mode: SyncMode) -> Self;
}

#[cfg(feature = "blk")]
pub enum DiskFormat {
    Raw,    // 0
    Qcow2,  // 1
    Vmdk,   // 2
}

#[cfg(feature = "blk")]
pub enum SyncMode {
    None,     // Ignore flush (risky)
    Relaxed,  // macOS-optimized
    Full,     // Strict sync (default)
}
```

**Maps to:** `BlockDeviceConfig`, `ImageType`, `SyncMode` in `src/vmm/src/vmm_config/block.rs`

---

### NetConfig (virtio-net)

```rust
#[cfg(feature = "net")]
pub struct NetBuilder { .. }

#[cfg(feature = "net")]
impl NetBuilder {
    pub fn id(self, id: &str) -> Self;
    pub fn backend(self, backend: NetBackend) -> Self;
    pub fn mac(self, mac: [u8; 6]) -> Self;
    pub fn features(self, features: NetFeatures) -> Self;
}

#[cfg(feature = "net")]
pub enum NetBackend {
    /// TAP device (Linux only)
    Tap { name: String },
    /// Unix stream socket (for passt)
    UnixStream { path: PathBuf },
    /// Unix stream socket from existing fd
    UnixStreamFd { fd: RawFd },
    /// Unix datagram socket (for gvproxy)
    UnixDatagram { path: PathBuf },
    /// Unix datagram socket from existing fd
    UnixDatagramFd { fd: RawFd },
}

#[cfg(feature = "net")]
bitflags! {
    pub struct NetFeatures: u32 {
        const CSUM        = 1 << 0;
        const GUEST_CSUM  = 1 << 1;
        const GUEST_TSO4  = 1 << 7;
        const GUEST_TSO6  = 1 << 8;
        const GUEST_UFO   = 1 << 10;
        const HOST_TSO4   = 1 << 11;
        const HOST_TSO6   = 1 << 12;
        const HOST_UFO    = 1 << 14;
    }
}
```

**Maps to:** `NetworkInterfaceConfig`, `VirtioNetBackend` in `src/vmm/src/vmm_config/net.rs`

---

### ConsoleConfig (virtio-console)

```rust
pub struct ConsoleBuilder { .. }

impl ConsoleBuilder {
    /// Add a TTY port (bidirectional terminal)
    pub fn tty(self, name: &str, fd: RawFd) -> Self;

    /// Add input/output port pair
    pub fn port(self, name: &str, input: RawFd, output: RawFd) -> Self;

    /// Add output-only port (e.g., stderr)
    pub fn output_port(self, name: &str, fd: RawFd) -> Self;
}
```

**Maps to:** `VirtioConsoleConfigMode`, `PortConfig` in `src/vmm/src/resources.rs`

---

### GpuConfig (virtio-gpu)

```rust
#[cfg(feature = "gpu")]
pub struct GpuBuilder { .. }

#[cfg(feature = "gpu")]
impl GpuBuilder {
    pub fn virgl_flags(self, flags: u32) -> Self;
    pub fn shm_size(self, size: usize) -> Self;
}

#[cfg(feature = "gpu")]
pub struct DisplayBuilder { .. }

#[cfg(feature = "gpu")]
impl DisplayBuilder {
    pub fn size(self, width: u32, height: u32) -> Self;
    pub fn refresh_rate(self, hz: u32) -> Self;
    pub fn dpi(self, dpi: u32) -> Self;
    pub fn physical_size_mm(self, width_mm: u16, height_mm: u16) -> Self;
    pub fn edid(self, edid_blob: &[u8]) -> Self;
}
```

**Maps to:** `DisplayInfo`, `EdidParams`, `PhysicalSize` in `src/devices/src/virtio/gpu/display.rs`

---

### VsockConfig

```rust
pub struct VsockBuilder { .. }

impl VsockBuilder {
    pub fn cid(self, cid: u32) -> Self;
    pub fn port(self, guest_port: u32, host_fd: RawFd) -> Self;
    pub fn tsi(self, enabled: bool) -> Self;
    pub fn tsi_unix(self, enabled: bool) -> Self;
    pub fn tsi_port_map(self, host: u16, guest: u16) -> Self;
}
```

**Maps to:** `VsockDeviceConfig` in `src/vmm/src/vmm_config/vsock.rs`

---

### ExecConfig

```rust
pub struct ExecBuilder { .. }

impl ExecBuilder {
    pub fn path(self, path: impl AsRef<Path>) -> Self;
    pub fn args<I, S>(self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
    pub fn env(self, key: &str, value: &str) -> Self;
    pub fn envs<I, K, V>(self, envs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>;
    pub fn workdir(self, path: impl AsRef<Path>) -> Self;
    pub fn rlimit(self, resource: &str, soft: u64, hard: u64) -> Self;
}
```

**Maps to:** Fields in `ContextConfig` (`exec_path`, `args`, `env`, `workdir`, `rlimits`)

---

## Error Types

```rust
pub enum Error {
    // Configuration errors
    InvalidVcpuCount(u8),
    InvalidMemorySize(u32),
    KernelNotFound(PathBuf),
    InitramfsNotFound(PathBuf),
    InvalidKernelFormat,

    // Device errors
    #[cfg(feature = "blk")]
    DiskNotFound(PathBuf),
    #[cfg(feature = "blk")]
    InvalidDiskFormat,
    #[cfg(feature = "net")]
    NetworkBackendError(String),

    // Runtime errors
    VmCreationFailed(String),
    VmRunFailed(String),

    // System errors
    Io(std::io::Error),
    EventFd(std::io::Error),
}
```

---

## Feature Flags

| Feature | Description | Configs Enabled |
|---------|-------------|-----------------|
| `blk` | Block device support | `DiskBuilder`, `DiskFormat`, `SyncMode` |
| `net` | Network support | `NetBuilder`, `NetBackend`, `NetFeatures` |
| `gpu` | GPU/display support | `GpuBuilder`, `DisplayBuilder` |
| `snd` | Sound support | `sound()` method |
| `input` | Input device support | Input backends |
| `tee` | TEE support | `TeeConfig` |
| `efi` | EFI firmware support | EFI boot path |
| `nitro` | AWS Nitro Enclaves | Nitro config |

---

## Internal Mapping

| Public Type | Internal Type | Location |
|-------------|---------------|----------|
| `MachineBuilder` | `VmConfig` | `vmm/vmm_config/machine_config.rs` |
| `KernelBuilder` | `ExternalKernel` | `vmm/vmm_config/external_kernel.rs` |
| `KernelFormat` | `KernelFormat` | `vmm/vmm_config/external_kernel.rs` |
| `FsBuilder` | `FsDeviceConfig` | `vmm/vmm_config/fs.rs` |
| `DiskBuilder` | `BlockDeviceConfig` | `vmm/vmm_config/block.rs` |
| `DiskFormat` | `ImageType` | `vmm/vmm_config/block.rs` |
| `SyncMode` | `SyncMode` | `vmm/vmm_config/block.rs` |
| `NetBuilder` | `NetworkInterfaceConfig` | `vmm/vmm_config/net.rs` |
| `NetBackend` | `VirtioNetBackend` | `vmm/vmm_config/net.rs` |
| `ConsoleBuilder` | `VirtioConsoleConfigMode` | `vmm/resources.rs` |
| `VsockBuilder` | `VsockDeviceConfig` | `vmm/vmm_config/vsock.rs` |
| `GpuBuilder` | GPU fields in `VmResources` | `vmm/resources.rs` |
| `DisplayBuilder` | `DisplayInfo` | `devices/virtio/gpu/display.rs` |
| `ExecBuilder` | `ContextConfig` fields | `libkrun/lib.rs` |
| `Vm` | `VmResources` + run logic | `vmm/resources.rs` |

---

## Implementation Steps

### Phase 1: Core Structure

1. Create `src/libkrun/src/config/` module structure
2. Define `Error` enum in `error.rs`
3. Implement `MachineBuilder` and `MachineConfig`
4. Implement `KernelBuilder`, `KernelConfig`, `KernelFormat`
5. Implement basic `VmBuilder` with machine + kernel support
6. Implement `Vm::run()` by extracting logic from `krun_start_enter()`

### Phase 2: Devices

7. Implement `FsBuilder` and `FsConfig`
8. Implement `ConsoleBuilder` and console configs
9. Implement `VsockBuilder` and vsock config
10. Implement `ExecBuilder` for execution environment

### Phase 3: Feature-Gated Devices

11. `#[cfg(feature = "blk")]` - `DiskBuilder`, `DiskFormat`, `SyncMode`
12. `#[cfg(feature = "net")]` - `NetBuilder`, `NetBackend`, `NetFeatures`
13. `#[cfg(feature = "gpu")]` - `GpuBuilder`, `DisplayBuilder`
14. `#[cfg(feature = "snd")]` - Sound support

### Phase 4: Cleanup

15. Remove C FFI functions (or gate behind `ffi` feature)
16. Remove global `CTX_MAP`
17. Update documentation
18. Add integration tests

---

## Example Usage

### Minimal VM

```rust
use libkrun::{VmBuilder, KernelFormat};

let vm = VmBuilder::new()
    .machine(|m| m.vcpus(1).memory_mib(512))
    .kernel(|k| k.path("/boot/vmlinuz").format(KernelFormat::Raw))
    .initramfs("/boot/initramfs.img")
    .fs(|fs| fs.tag("root").path("/"))
    .exec(|e| e.path("/bin/sh"))
    .build()?;

let exit_code = vm.run()?;
```

### Full-Featured VM

```rust
use libkrun::{VmBuilder, KernelFormat, DiskFormat, SyncMode, NetBackend};

let vm = VmBuilder::new()
    .machine(|m| m
        .vcpus(4)
        .memory_mib(4096)
        .nested_virt(true))
    .kernel(|k| k
        .path("/path/to/vmlinux")
        .format(KernelFormat::Raw)
        .cmdline("console=hvc0"))
    .initramfs("/path/to/initramfs.cpio.gz")
    .fs(|fs| fs.tag("shared").path("/home/user/shared"))
    .disk(|d| d
        .id("root")
        .path("/path/to/disk.qcow2")
        .format(DiskFormat::Qcow2)
        .sync_mode(SyncMode::Full))
    .net(|n| n
        .id("eth0")
        .backend(NetBackend::Tap { name: "tap0".into() })
        .mac([0x52, 0x54, 0x00, 0x12, 0x34, 0x56]))
    .console(|c| c
        .tty("console", libc::STDIN_FILENO)
        .output_port("stderr", libc::STDERR_FILENO))
    .gpu(|g| g.virgl_flags(0))
    .display(1920, 1080)
    .sound(true)
    .exec(|e| e
        .path("/usr/bin/app")
        .args(["--config", "/etc/app.conf"])
        .env("DISPLAY", ":0")
        .workdir("/home/user"))
    .uid(1000)
    .gid(1000)
    .build()?;

let exit_code = vm.run()?;
```

---

## Extensible Backend Design

### Overview

The current implementation has two key traits that can be leveraged for extensibility:

1. **`FileSystem`** trait (`devices/src/virtio/fs/filesystem.rs`) - For custom filesystem implementations
2. **`NetBackend`** trait (`devices/src/virtio/net/backend.rs`) - For custom network backends

However, both are currently hardcoded in device construction:
- `Fs` device always creates `PassthroughFs`
- `Net` device uses `VirtioNetBackend` enum (closed set)

### Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        VmBuilder                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│   FsDeviceConfig { fs_id, shared_dir, shm_size }           │
│   NetworkInterfaceConfig { backend: VirtioNetBackend, .. }  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│   Fs::new() → creates PassthroughFs internally              │
│   Net::new() → matches VirtioNetBackend enum                │
└─────────────────────────────────────────────────────────────┘
```

### Proposed Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        VmBuilder                            │
└─────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            ▼                                   ▼
┌───────────────────────┐           ┌───────────────────────┐
│  Built-in Backends    │           │  Custom Backends      │
│  (passthrough, tap,   │           │  (user-provided       │
│   unixstream, etc.)   │           │   Box<dyn Trait>)     │
└───────────────────────┘           └───────────────────────┘
            │                                   │
            └─────────────────┬─────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│   Fs::with_backend(Box<dyn FileSystem + Send + Sync>)       │
│   Net::with_backend(Box<dyn NetBackend + Send>)             │
└─────────────────────────────────────────────────────────────┘
```

---

### Network Backend Extensibility

#### The `NetBackend` Trait (existing)

```rust
// devices/src/virtio/net/backend.rs
pub trait NetBackend: Send {
    /// Read an ethernet frame from the backend
    fn read_frame(&mut self, buf: &mut [u8]) -> Result<usize, ReadError>;

    /// Write an ethernet frame to the backend
    /// `hdr_len` bytes at start of `buf` can be overwritten for framing
    fn write_frame(&mut self, hdr_len: usize, buf: &mut [u8]) -> Result<(), WriteError>;

    /// Check if a partial write is pending
    fn has_unfinished_write(&self) -> bool;

    /// Complete a partial write
    fn try_finish_write(&mut self, hdr_len: usize, buf: &[u8]) -> Result<(), WriteError>;

    /// Get the raw fd for epoll registration
    fn raw_socket_fd(&self) -> RawFd;
}
```

#### Changes Required

1. **Modify `Net` device** to accept `Box<dyn NetBackend>`:

```rust
// devices/src/virtio/net/device.rs
impl Net {
    /// Create with a built-in backend (existing behavior)
    pub fn new(
        id: String,
        cfg_backend: VirtioNetBackend,  // Enum for built-ins
        mac: [u8; 6],
        features: u32,
    ) -> Result<Self>;

    /// Create with a custom backend (NEW)
    pub fn with_backend(
        id: String,
        backend: Box<dyn NetBackend + Send>,
        mac: [u8; 6],
        features: u32,
    ) -> Result<Self>;
}
```

2. **Update `NetWorker`** to store the backend directly instead of enum:

```rust
// Current
pub struct NetWorker {
    backend: Box<dyn NetBackend + Send>,  // Already uses trait object!
    // ...
}

// NetWorker::new() currently matches on VirtioNetBackend enum
// Change to accept Box<dyn NetBackend> directly
```

3. **Public API**:

```rust
// Built-in backends
pub enum NetBackend {
    Tap { name: String },
    UnixStream { path: PathBuf },
    UnixStreamFd { fd: RawFd },
    UnixDatagram { path: PathBuf },
    UnixDatagramFd { fd: RawFd },
    /// User-provided custom backend
    Custom(Box<dyn crate::NetBackendImpl + Send>),
}

// Re-export the trait for users to implement
pub use devices::virtio::net::backend::NetBackend as NetBackendImpl;
```

4. **Builder API**:

```rust
impl NetBuilder {
    // Existing
    pub fn backend(self, backend: NetBackend) -> Self;

    // New: accept any impl (consistent with FsBuilder::custom)
    pub fn custom<B>(self, backend: B) -> Self
    where
        B: NetBackendImpl + Send + 'static;
}
```

#### Example: Custom Network Backend

```rust
use libkrun::NetBackendImpl;
use std::os::fd::RawFd;

pub struct MyNetworkBackend {
    socket: std::net::UdpSocket,
}

impl NetBackendImpl for MyNetworkBackend {
    fn read_frame(&mut self, buf: &mut [u8]) -> Result<usize, ReadError> {
        match self.socket.recv(buf) {
            Ok(n) => Ok(n),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                Err(ReadError::NothingRead)
            }
            Err(e) => Err(ReadError::Internal(/* convert error */)),
        }
    }

    fn write_frame(&mut self, hdr_len: usize, buf: &mut [u8]) -> Result<(), WriteError> {
        let frame = &buf[hdr_len..];
        match self.socket.send(frame) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                Err(WriteError::NothingWritten)
            }
            Err(_) => Err(WriteError::ProcessNotRunning),
        }
    }

    fn has_unfinished_write(&self) -> bool { false }

    fn try_finish_write(&mut self, _: usize, _: &[u8]) -> Result<(), WriteError> { Ok(()) }

    fn raw_socket_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd;
        self.socket.as_raw_fd()
    }
}

// Usage
let vm = VmBuilder::new()
    .net(|n| n
        .id("eth0")
        .custom(MyNetworkBackend::new()?)
        .mac([0x52, 0x54, 0x00, 0x12, 0x34, 0x56]))
    .build()?;
```

---

### Filesystem Backend Extensibility

#### The `FileSystem` Trait (existing, simplified)

```rust
// devices/src/virtio/fs/filesystem.rs
pub trait FileSystem {
    /// Inode type (must convert to/from u64)
    type Inode: From<u64> + Into<u64>;

    /// Handle type for open files/directories
    type Handle: From<u64> + Into<u64>;

    // Lifecycle
    fn init(&self, capable: FsOptions) -> io::Result<FsOptions>;
    fn destroy(&self);

    // Inode operations
    fn lookup(&self, ctx: Context, parent: Self::Inode, name: &CStr) -> io::Result<Entry>;
    fn forget(&self, ctx: Context, inode: Self::Inode, count: u64);
    fn getattr(&self, ctx: Context, inode: Self::Inode, handle: Option<Self::Handle>)
        -> io::Result<(stat64, Duration)>;
    fn setattr(&self, ctx: Context, inode: Self::Inode, attr: stat64,
               handle: Option<Self::Handle>, valid: SetattrValid)
        -> io::Result<(stat64, Duration)>;

    // File operations
    fn open(&self, ctx: Context, inode: Self::Inode, flags: u32, fuse_flags: u32)
        -> io::Result<(Option<Self::Handle>, OpenOptions)>;
    fn read<W: ZeroCopyWriter>(&self, ctx: Context, inode: Self::Inode,
            handle: Self::Handle, w: W, size: u32, offset: u64, ...) -> io::Result<usize>;
    fn write<R: ZeroCopyReader>(&self, ctx: Context, inode: Self::Inode,
             handle: Self::Handle, r: R, size: u32, offset: u64, ...) -> io::Result<usize>;
    fn release(&self, ctx: Context, inode: Self::Inode, flags: u32,
               handle: Self::Handle, ...) -> io::Result<()>;

    // Directory operations
    fn opendir(&self, ctx: Context, inode: Self::Inode, flags: u32)
        -> io::Result<(Option<Self::Handle>, OpenOptions)>;
    fn readdir(&self, ctx: Context, inode: Self::Inode, handle: Self::Handle,
               size: u32, offset: u64) -> io::Result<Vec<DirEntry>>;
    fn releasedir(&self, ctx: Context, inode: Self::Inode, flags: u32,
                  handle: Self::Handle) -> io::Result<()>;

    // Many more methods with default ENOSYS implementations...
    fn mkdir(...) -> io::Result<Entry> { Err(ENOSYS) }
    fn rmdir(...) -> io::Result<()> { Err(ENOSYS) }
    fn create(...) -> io::Result<(Entry, Option<Handle>, OpenOptions)> { Err(ENOSYS) }
    fn unlink(...) -> io::Result<()> { Err(ENOSYS) }
    fn rename(...) -> io::Result<()> { Err(ENOSYS) }
    fn link(...) -> io::Result<Entry> { Err(ENOSYS) }
    fn symlink(...) -> io::Result<Entry> { Err(ENOSYS) }
    fn readlink(...) -> io::Result<Vec<u8>> { Err(ENOSYS) }
    // ... 30+ more methods
}
```

#### Challenge: Associated Types

The `FileSystem` trait has associated types (`Inode`, `Handle`), making it non-object-safe.
The `Server<F: FileSystem + Sync>` is generic over `F`.

**Solution: Type-erased wrapper**

```rust
// New: Object-safe wrapper trait
pub trait DynFileSystem: Send + Sync {
    fn init(&self, capable: FsOptions) -> io::Result<FsOptions>;
    fn destroy(&self);
    fn lookup(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<Entry>;
    fn forget(&self, ctx: Context, inode: u64, count: u64);
    fn getattr(&self, ctx: Context, inode: u64, handle: Option<u64>)
        -> io::Result<(stat64, Duration)>;
    // ... all methods with u64 for Inode/Handle
}

// Blanket impl for any FileSystem
impl<F: FileSystem + Send + Sync> DynFileSystem for F
where
    F::Inode: Send + Sync,
    F::Handle: Send + Sync,
{
    fn lookup(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<Entry> {
        FileSystem::lookup(self, ctx, F::Inode::from(parent), name)
    }
    // ... delegate all methods
}
```

#### Changes Required

1. **Create `DynFileSystem` trait** (object-safe version)

2. **Modify `Fs` device**:

```rust
impl Fs {
    /// Create with built-in passthrough (existing)
    pub fn new(fs_id: String, shared_dir: String, exit_code: Arc<AtomicI32>)
        -> Result<Fs>;

    /// Create with custom filesystem (NEW)
    pub fn with_filesystem(
        fs_id: String,
        filesystem: Box<dyn DynFileSystem>,
        exit_code: Arc<AtomicI32>,
    ) -> Result<Fs>;
}
```

3. **Modify `FsWorker` and `Server`**:

```rust
// Option A: Make Server use DynFileSystem
pub struct Server {
    fs: Box<dyn DynFileSystem>,
    // ...
}

// Option B: Create DynServer wrapper
pub struct DynServer {
    fs: Box<dyn DynFileSystem>,
}

impl DynServer {
    pub fn handle_message(&self, r: Reader, w: Writer, ...) -> Result<usize> {
        // Dispatch to fs methods using u64 for inode/handle
    }
}
```

4. **Public API**:

```rust
// Re-export for users
pub use devices::virtio::fs::filesystem::{
    FileSystem as FileSystemImpl,
    DynFileSystem,
    Context, Entry, DirEntry, FsOptions, OpenOptions,
    ZeroCopyReader, ZeroCopyWriter,
};

// Builder
impl FsBuilder {
    // Existing: passthrough
    pub fn path(self, path: impl AsRef<Path>) -> Self;

    // New: custom filesystem
    pub fn custom<F>(self, filesystem: F) -> Self
    where
        F: FileSystemImpl + Send + Sync + 'static;

    // Or with trait object directly
    pub fn custom_boxed(self, filesystem: Box<dyn DynFileSystem>) -> Self;
}
```

#### Example: Custom Filesystem (In-Memory)

```rust
use libkrun::fs::{FileSystemImpl, Context, Entry, FsOptions, DirEntry};
use std::collections::HashMap;
use std::ffi::CStr;
use std::io;
use std::sync::RwLock;

pub struct MemFs {
    inodes: RwLock<HashMap<u64, MemInode>>,
    next_inode: AtomicU64,
}

struct MemInode {
    kind: InodeKind,
    data: Vec<u8>,
    // ...
}

impl FileSystemImpl for MemFs {
    type Inode = u64;
    type Handle = u64;

    fn init(&self, capable: FsOptions) -> io::Result<FsOptions> {
        Ok(FsOptions::empty())
    }

    fn lookup(&self, ctx: Context, parent: u64, name: &CStr) -> io::Result<Entry> {
        let inodes = self.inodes.read().unwrap();
        // Look up child in parent directory
        // Return Entry with inode, generation, attr
    }

    fn getattr(&self, ctx: Context, inode: u64, _: Option<u64>)
        -> io::Result<(stat64, Duration)>
    {
        let inodes = self.inodes.read().unwrap();
        let inode_data = inodes.get(&inode).ok_or(ENOENT)?;
        Ok((inode_data.to_stat64(), Duration::from_secs(1)))
    }

    fn read<W: ZeroCopyWriter>(
        &self, ctx: Context, inode: u64, handle: u64,
        mut w: W, size: u32, offset: u64, ...
    ) -> io::Result<usize> {
        let inodes = self.inodes.read().unwrap();
        let data = &inodes.get(&inode).ok_or(ENOENT)?.data;
        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, data.len());
        w.write(&data[start..end])
    }

    // Implement other required methods...
}

// Usage
let vm = VmBuilder::new()
    .fs(|fs| fs
        .tag("mem")
        .custom(MemFs::new()))
    .build()?;
```

---

### Summary of Changes

#### Files to Modify

| File | Changes |
|------|---------|
| `devices/src/virtio/net/device.rs` | Add `with_backend()` constructor |
| `devices/src/virtio/net/worker.rs` | Accept `Box<dyn NetBackend>` directly |
| `devices/src/virtio/net/mod.rs` | Re-export `NetBackend` trait |
| `devices/src/virtio/fs/filesystem.rs` | Add `DynFileSystem` trait |
| `devices/src/virtio/fs/device.rs` | Add `with_filesystem()` constructor |
| `devices/src/virtio/fs/server.rs` | Support `DynFileSystem` or add `DynServer` |
| `devices/src/virtio/fs/worker.rs` | Use dynamic dispatch |
| `libkrun/src/lib.rs` | Re-export traits for external use |
| `libkrun/src/config/net.rs` | Add `custom_backend()` to builder |
| `libkrun/src/config/fs.rs` | Add `custom()` to builder |

#### New Public Types

```rust
// Network
pub trait NetBackendImpl: Send { ... }
pub struct ReadError { ... }
pub struct WriteError { ... }

// Filesystem
pub trait FileSystemImpl { ... }
pub trait DynFileSystem: Send + Sync { ... }
pub struct Context { ... }
pub struct Entry { ... }
pub struct DirEntry<'a> { ... }
pub struct FsOptions { ... }
pub struct OpenOptions { ... }
pub trait ZeroCopyReader { ... }
pub trait ZeroCopyWriter { ... }
```

#### Implementation Priority

1. **Network backend** - Simpler, trait already object-safe
2. **Filesystem backend** - More complex due to associated types
