//! Sub-builders for VmBuilder nested configuration.

use std::path::{Path, PathBuf};

#[cfg(not(any(feature = "tee", feature = "nitro")))]
use crate::backends::fs::DynFileSystem;

#[cfg(feature = "net")]
use crate::backends::net::NetBackend;

//--------------------------------------------------------------------------------------------------
// Types: Machine Builder
//--------------------------------------------------------------------------------------------------

/// Builder for machine configuration (vCPUs, memory, etc.).
#[derive(Debug, Clone)]
pub struct MachineBuilder {
    pub(crate) vcpus: u8,
    pub(crate) memory_mib: usize,
    pub(crate) hyperthreading: bool,
    pub(crate) nested_virt: bool,
}

//--------------------------------------------------------------------------------------------------
// Types: Kernel Builder
//--------------------------------------------------------------------------------------------------

/// Builder for kernel configuration.
#[derive(Debug, Clone, Default)]
pub struct KernelBuilder {
    pub(crate) cmdline: Option<String>,
}

//--------------------------------------------------------------------------------------------------
// Types: Filesystem Builder
//--------------------------------------------------------------------------------------------------

/// Builder for filesystem configuration.
pub struct FsBuilder {
    pub(crate) configs: Vec<FsConfig>,
    current_tag: Option<String>,
    current_shm_size: Option<usize>,
}

/// Configuration for a single filesystem mount.
pub enum FsConfig {
    /// Path-based filesystem (passthrough).
    Path {
        tag: String,
        path: PathBuf,
        shm_size: Option<usize>,
    },
    /// Custom filesystem backend.
    #[cfg(not(any(feature = "tee", feature = "nitro")))]
    Custom {
        tag: String,
        backend: Box<dyn DynFileSystem + Send + Sync>,
    },
}

//--------------------------------------------------------------------------------------------------
// Types: Network Builder
//--------------------------------------------------------------------------------------------------

/// Builder for network configuration.
#[cfg(feature = "net")]
pub struct NetBuilder {
    pub(crate) configs: Vec<NetConfig>,
    current_mac: Option<[u8; 6]>,
}

/// Configuration for a single network device.
#[cfg(feature = "net")]
pub enum NetConfig {
    /// Custom network backend.
    Custom {
        mac: Option<[u8; 6]>,
        backend: Box<dyn NetBackend + Send>,
    },
}

//--------------------------------------------------------------------------------------------------
// Types: Console Builder
//--------------------------------------------------------------------------------------------------

/// Builder for console/output configuration.
#[derive(Debug, Clone, Default)]
pub struct ConsoleBuilder {
    pub(crate) output: Option<PathBuf>,
    #[cfg(feature = "snd")]
    pub(crate) sound: bool,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_virgl_flags: Option<u32>,
    #[cfg(feature = "gpu")]
    pub(crate) gpu_shm_size: Option<usize>,
}

//--------------------------------------------------------------------------------------------------
// Types: Exec Builder
//--------------------------------------------------------------------------------------------------

/// Builder for execution configuration.
#[derive(Debug, Clone, Default)]
pub struct ExecBuilder {
    pub(crate) path: Option<String>,
    pub(crate) args: Vec<String>,
    pub(crate) env: Vec<(String, String)>,
    pub(crate) workdir: Option<String>,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) rlimits: Vec<(String, u64, u64)>,
}

//--------------------------------------------------------------------------------------------------
// Types: Disk Builder
//--------------------------------------------------------------------------------------------------

/// Builder for block device configuration.
#[cfg(feature = "blk")]
#[derive(Debug, Clone, Default)]
pub struct DiskBuilder {
    pub(crate) configs: Vec<DiskConfig>,
    current_path: Option<PathBuf>,
    current_read_only: bool,
}

/// Configuration for a single block device.
#[cfg(feature = "blk")]
#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub read_only: bool,
}

//--------------------------------------------------------------------------------------------------
// Methods: Machine Builder
//--------------------------------------------------------------------------------------------------

impl MachineBuilder {
    /// Create a new machine builder with defaults.
    pub fn new() -> Self {
        Self {
            vcpus: 1,
            memory_mib: 512,
            hyperthreading: false,
            nested_virt: false,
        }
    }

    /// Set the number of virtual CPUs.
    pub fn vcpus(mut self, count: u8) -> Self {
        self.vcpus = count;
        self
    }

    /// Set the memory size in MiB.
    pub fn memory_mib(mut self, mib: usize) -> Self {
        self.memory_mib = mib;
        self
    }

    /// Enable or disable hyperthreading.
    pub fn hyperthreading(mut self, enabled: bool) -> Self {
        self.hyperthreading = enabled;
        self
    }

    /// Enable or disable nested virtualization.
    pub fn nested_virt(mut self, enabled: bool) -> Self {
        self.nested_virt = enabled;
        self
    }
}

impl Default for MachineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Kernel Builder
//--------------------------------------------------------------------------------------------------

impl KernelBuilder {
    /// Create a new kernel builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append to the kernel command line.
    pub fn cmdline(mut self, cmdline: &str) -> Self {
        if let Some(ref mut existing) = self.cmdline {
            existing.push(' ');
            existing.push_str(cmdline);
        } else {
            self.cmdline = Some(cmdline.to_string());
        }
        self
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Filesystem Builder
//--------------------------------------------------------------------------------------------------

impl FsBuilder {
    /// Create a new filesystem builder.
    pub fn new() -> Self {
        Self {
            configs: Vec::new(),
            current_tag: None,
            current_shm_size: None,
        }
    }

    /// Set the root filesystem path (tag: "krun_root").
    pub fn root(mut self, path: impl AsRef<Path>) -> Self {
        self.configs.push(FsConfig::Path {
            tag: "krun_root".to_string(),
            path: path.as_ref().to_path_buf(),
            shm_size: None,
        });
        self
    }

    /// Set the tag for the next mount.
    pub fn tag(mut self, tag: &str) -> Self {
        self.current_tag = Some(tag.to_string());
        self
    }

    /// Set the path for a filesystem mount.
    ///
    /// If `tag()` was called previously, uses that tag; otherwise generates one.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        let tag = self
            .current_tag
            .take()
            .unwrap_or_else(|| format!("fs{}", self.configs.len()));
        let shm_size = self.current_shm_size.take();

        self.configs.push(FsConfig::Path {
            tag,
            path: path.as_ref().to_path_buf(),
            shm_size,
        });
        self
    }

    /// Set the DAX shared memory size for the next mount.
    pub fn shm_size(mut self, size: usize) -> Self {
        self.current_shm_size = Some(size);
        self
    }

    /// Use a custom filesystem backend.
    #[cfg(not(any(feature = "tee", feature = "nitro")))]
    pub fn custom(mut self, backend: Box<dyn DynFileSystem + Send + Sync>) -> Self {
        let tag = self
            .current_tag
            .take()
            .unwrap_or_else(|| format!("fs{}", self.configs.len()));

        self.configs.push(FsConfig::Custom { tag, backend });
        self
    }
}

impl Default for FsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Network Builder
//--------------------------------------------------------------------------------------------------

#[cfg(feature = "net")]
impl NetBuilder {
    /// Create a new network builder.
    pub fn new() -> Self {
        Self {
            configs: Vec::new(),
            current_mac: None,
        }
    }

    /// Set the MAC address for the next network device.
    pub fn mac(mut self, mac: [u8; 6]) -> Self {
        self.current_mac = Some(mac);
        self
    }

    /// Use a custom network backend.
    pub fn custom(mut self, backend: Box<dyn NetBackend + Send>) -> Self {
        let mac = self.current_mac.take();
        self.configs.push(NetConfig::Custom { mac, backend });
        self
    }
}

#[cfg(feature = "net")]
impl Default for NetBuilder {
    fn default() -> Self {
        Self::new()
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Console Builder
//--------------------------------------------------------------------------------------------------

impl ConsoleBuilder {
    /// Create a new console builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path to send console output.
    pub fn output(mut self, path: impl AsRef<Path>) -> Self {
        self.output = Some(path.as_ref().to_path_buf());
        self
    }

    /// Enable the virtio-snd device.
    #[cfg(feature = "snd")]
    pub fn sound(mut self, enabled: bool) -> Self {
        self.sound = enabled;
        self
    }

    /// Set GPU virgl flags.
    #[cfg(feature = "gpu")]
    pub fn gpu_virgl_flags(mut self, flags: u32) -> Self {
        self.gpu_virgl_flags = Some(flags);
        self
    }

    /// Set GPU shared memory size.
    #[cfg(feature = "gpu")]
    pub fn gpu_shm_size(mut self, size: usize) -> Self {
        self.gpu_shm_size = Some(size);
        self
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Exec Builder
//--------------------------------------------------------------------------------------------------

impl ExecBuilder {
    /// Create a new exec builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the executable path.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_string_lossy().to_string());
        self
    }

    /// Set command line arguments.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.args = args.into_iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Add a single environment variable.
    pub fn env(mut self, key: &str, value: &str) -> Self {
        self.env.push((key.to_string(), value.to_string()));
        self
    }

    /// Add multiple environment variables.
    pub fn envs<I, K, V>(mut self, envs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.env.extend(
            envs.into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string())),
        );
        self
    }

    /// Set the working directory.
    pub fn workdir(mut self, path: impl AsRef<Path>) -> Self {
        self.workdir = Some(path.as_ref().to_string_lossy().to_string());
        self
    }

    /// Set the user ID.
    pub fn uid(mut self, uid: u32) -> Self {
        self.uid = Some(uid);
        self
    }

    /// Set the group ID.
    pub fn gid(mut self, gid: u32) -> Self {
        self.gid = Some(gid);
        self
    }

    /// Set a resource limit.
    pub fn rlimit(mut self, resource: &str, soft: u64, hard: u64) -> Self {
        self.rlimits
            .push((resource.to_string(), soft, hard));
        self
    }
}

//--------------------------------------------------------------------------------------------------
// Methods: Disk Builder
//--------------------------------------------------------------------------------------------------

#[cfg(feature = "blk")]
impl DiskBuilder {
    /// Create a new disk builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the path for a block device.
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        // Finalize any pending config
        if let Some(pending_path) = self.current_path.take() {
            self.configs.push(DiskConfig {
                path: pending_path,
                read_only: self.current_read_only,
            });
            self.current_read_only = false;
        }

        self.current_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set read-only mode for the current disk.
    pub fn read_only(mut self, ro: bool) -> Self {
        self.current_read_only = ro;
        self
    }

    /// Finalize the builder (called internally).
    pub(crate) fn finalize(mut self) -> Self {
        if let Some(path) = self.current_path.take() {
            self.configs.push(DiskConfig {
                path,
                read_only: self.current_read_only,
            });
        }
        self
    }
}
