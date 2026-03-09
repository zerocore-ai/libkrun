//! VM Builder for creating and configuring microVMs using nested builders.

#[cfg(not(any(feature = "tee", feature = "aws-nitro")))]
use std::sync::Arc;

use vmm::resources::{VirtioConsoleConfigMode, VmResources};
use vmm::vmm_config::machine_config::VmConfig;

#[cfg(not(any(feature = "tee", feature = "aws-nitro")))]
use vmm::vmm_config::fs::CustomFsDeviceConfig;
#[cfg(not(feature = "tee"))]
use vmm::vmm_config::fs::FsDeviceConfig;

#[cfg(feature = "blk")]
use super::builders::DiskBuilder;
#[cfg(not(any(feature = "tee", feature = "aws-nitro")))]
use super::builders::FsConfig;
#[cfg(feature = "net")]
use super::builders::NetBuilder;
use super::builders::{ConsoleBuilder, ExecBuilder, FsBuilder, KernelBuilder, MachineBuilder};

use super::error::{ConfigError, Error, Result};
use super::vm::Vm;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Builder for creating and configuring a microVM.
///
/// Uses nested builders for organized configuration:
///
/// # Example
///
/// ```rust,no_run
/// use msb_krun::VmBuilder;
///
/// let vm = VmBuilder::new()
///     .machine(|m| m.vcpus(4).memory_mib(2048))
///     .fs(|fs| fs.root("/path/to/rootfs"))
///     .exec(|e| e.path("/bin/myapp").args(["--flag"]).env("HOME", "/root"))
///     .build()
///     .expect("Failed to build VM");
/// ```
pub struct VmBuilder {
    machine: MachineBuilder,
    kernel: KernelBuilder,
    #[cfg_attr(feature = "tee", allow(dead_code))]
    fs: FsBuilder,
    console: ConsoleBuilder,
    exec: ExecBuilder,
    #[cfg(feature = "net")]
    net: NetBuilder,
    #[cfg(feature = "blk")]
    disk: DiskBuilder,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl VmBuilder {
    /// Create a new VM builder with default configuration.
    ///
    /// Defaults:
    /// - 1 vCPU
    /// - 512 MiB memory
    /// - Hyperthreading disabled
    /// - Nested virtualization disabled
    pub fn new() -> Self {
        Self {
            machine: MachineBuilder::new(),
            kernel: KernelBuilder::new(),
            fs: FsBuilder::new(),
            console: ConsoleBuilder::new(),
            exec: ExecBuilder::new(),
            #[cfg(feature = "net")]
            net: NetBuilder::new(),
            #[cfg(feature = "blk")]
            disk: DiskBuilder::new(),
        }
    }

    /// Configure machine settings (vCPUs, memory, etc.).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .machine(|m| {
    ///         m.vcpus(4)
    ///             .memory_mib(2048)
    ///             .hyperthreading(true)
    ///             .nested_virt(true)
    ///     });
    /// ```
    pub fn machine(mut self, f: impl FnOnce(MachineBuilder) -> MachineBuilder) -> Self {
        self.machine = f(self.machine);
        self
    }

    /// Configure kernel settings.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .kernel(|k| {
    ///         k.krunfw_path("/path/to/libkrunfw.dylib")
    ///             .cmdline("debug")
    ///     });
    /// ```
    pub fn kernel(mut self, f: impl FnOnce(KernelBuilder) -> KernelBuilder) -> Self {
        self.kernel = f(self.kernel);
        self
    }

    /// Configure filesystem mounts.
    ///
    /// Can be called multiple times to add multiple mounts.
    ///
    /// # Examples
    ///
    /// Root filesystem only:
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .fs(|fs| fs.root("/path/to/rootfs"));
    /// ```
    ///
    /// Root filesystem with additional named mounts:
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .fs(|fs| fs.root("/path/to/rootfs"))
    ///     .fs(|fs| fs.tag("data").shm_size(1 << 30).path("/host/data"))
    ///     .fs(|fs| fs.tag("logs").path("/host/logs"));
    /// ```
    ///
    /// Custom filesystem backend:
    ///
    /// ```rust,ignore
    /// VmBuilder::new()
    ///     .fs(|fs| fs.tag("myfs").custom(Box::new(my_backend)));
    /// ```
    #[cfg(not(feature = "tee"))]
    pub fn fs(mut self, f: impl FnOnce(FsBuilder) -> FsBuilder) -> Self {
        let new_fs = f(FsBuilder::new());
        self.fs.configs.extend(new_fs.configs);
        self
    }

    /// Configure network devices.
    ///
    /// Can be called multiple times to add multiple devices.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// VmBuilder::new()
    ///     .net(|n| n.mac([0x52, 0x54, 0x00, 0x12, 0x34, 0x56]).custom(my_backend));
    /// ```
    #[cfg(feature = "net")]
    pub fn net(mut self, f: impl FnOnce(NetBuilder) -> NetBuilder) -> Self {
        let new_net = f(NetBuilder::new());
        self.net.configs.extend(new_net.configs);
        self
    }

    /// Configure block devices.
    ///
    /// Can be called multiple times to add multiple devices.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .disk(|d| d.path("/path/to/disk.img").read_only(true));
    /// ```
    #[cfg(feature = "blk")]
    pub fn disk(mut self, f: impl FnOnce(DiskBuilder) -> DiskBuilder) -> Self {
        let new_disk = f(DiskBuilder::new()).finalize();
        self.disk.configs.extend(new_disk.configs);
        self
    }

    /// Configure console and output settings.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .console(|c| c.output("/tmp/vm.log"));
    /// ```
    ///
    /// With the `gpu` and `snd` features:
    ///
    /// ```rust,ignore
    /// VmBuilder::new()
    ///     .console(|c| {
    ///         c.output("/tmp/vm.log")
    ///             .sound(true)
    ///             .gpu_virgl_flags(0x1)
    ///             .gpu_shm_size(1 << 28)
    ///     });
    /// ```
    pub fn console(mut self, f: impl FnOnce(ConsoleBuilder) -> ConsoleBuilder) -> Self {
        self.console = f(self.console);
        self
    }

    /// Configure execution settings.
    ///
    /// # Examples
    ///
    /// Setting environment variables one at a time with `.env()`:
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .exec(|e| {
    ///         e.path("/bin/myapp")
    ///             .args(["--flag", "value"])
    ///             .env("HOME", "/root")
    ///             .env("LANG", "en_US.UTF-8")
    ///             .workdir("/app")
    ///             .uid(1000)
    ///             .gid(1000)
    ///             .rlimit("NOFILE", 1024, 4096)
    ///     });
    /// ```
    ///
    /// Setting environment variables in bulk with `.envs()`:
    ///
    /// ```rust,no_run
    /// # use msb_krun::VmBuilder;
    /// VmBuilder::new()
    ///     .exec(|e| {
    ///         e.path("/bin/myapp")
    ///             .envs([("HOME", "/root"), ("LANG", "en_US.UTF-8")])
    ///     });
    /// ```
    pub fn exec(mut self, f: impl FnOnce(ExecBuilder) -> ExecBuilder) -> Self {
        self.exec = f(self.exec);
        self
    }

    /// Build the VM.
    ///
    /// This validates the configuration and creates a `Vm` instance ready to run.
    pub fn build(self) -> Result<Vm> {
        // Validate configuration
        if self.machine.vcpus == 0 {
            return Err(Error::Config(ConfigError::InvalidVcpuCount(0)));
        }

        if self.machine.memory_mib == 0 {
            return Err(Error::Config(ConfigError::InvalidMemorySize(0)));
        }

        // Build VmResources
        let mut vmr = VmResources::default();

        // Apply machine configuration
        let vm_config = VmConfig {
            vcpu_count: Some(self.machine.vcpus),
            mem_size_mib: Some(self.machine.memory_mib),
            ht_enabled: Some(self.machine.hyperthreading),
            ..Default::default()
        };
        let _ = vmr.set_vm_config(&vm_config);
        vmr.nested_enabled = self.machine.nested_virt;

        // Apply kernel configuration
        if let Some(cmdline) = self.kernel.cmdline {
            vmr.kernel_cmdline.epilog = Some(cmdline);
        }

        // Apply filesystem configuration
        #[cfg(not(feature = "tee"))]
        for config in self.fs.configs {
            match config {
                FsConfig::Path {
                    tag,
                    path,
                    shm_size,
                } => {
                    let fs_config = FsDeviceConfig {
                        fs_id: tag,
                        shared_dir: path.to_string_lossy().to_string(),
                        shm_size,
                        allow_root_dir_delete: false,
                    };
                    vmr.fs.push(fs_config);
                }
                #[cfg(not(any(feature = "tee", feature = "aws-nitro")))]
                FsConfig::Custom { tag, backend } => {
                    let backend: Box<dyn devices::virtio::fs::DynFileSystem> = backend;
                    let custom_config = CustomFsDeviceConfig {
                        fs_id: tag,
                        backend: Arc::from(backend),
                        shm_size: None,
                    };
                    vmr.custom_fs.push(custom_config);
                }
            }
        }

        // Apply console configuration
        if let Some(output) = self.console.output {
            vmr.console_output = Some(output);
        }

        #[cfg(feature = "snd")]
        {
            vmr.snd_device = self.console.sound;
        }

        #[cfg(feature = "gpu")]
        {
            vmr.gpu_virgl_flags = self.console.gpu_virgl_flags;
            vmr.gpu_shm_size = self.console.gpu_shm_size;
        }

        // Apply console port configuration
        if !self.console.ports.is_empty() {
            vmr.virtio_consoles
                .push(VirtioConsoleConfigMode::Explicit(self.console.ports));
        }

        if self.console.disable_implicit {
            vmr.disable_implicit_console = true;
        }

        // Format execution configuration
        let exec_path = self.exec.path;

        let args = if self.exec.args.is_empty() {
            None
        } else {
            Some(
                self.exec
                    .args
                    .iter()
                    .map(|s| format!("\"{}\"", s))
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        };

        let env = if self.exec.env.is_empty() {
            None
        } else {
            Some(
                self.exec
                    .env
                    .iter()
                    .map(|(k, v)| format!(" {}=\"{}\"", k, v))
                    .collect::<String>(),
            )
        };

        let rlimits = if self.exec.rlimits.is_empty() {
            None
        } else {
            Some(
                self.exec
                    .rlimits
                    .iter()
                    .map(|(r, s, h)| format!("{}:{}:{}", r, s, h))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        Ok(Vm::new(
            vmr,
            exec_path,
            args,
            env,
            self.exec.workdir,
            rlimits,
            self.exec.uid,
            self.exec.gid,
            self.kernel.krunfw_path,
            self.kernel.init_path,
        ))
    }
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl Default for VmBuilder {
    fn default() -> Self {
        Self::new()
    }
}
