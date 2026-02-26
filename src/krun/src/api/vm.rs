//! VM handle for running microVMs.

#[cfg(target_os = "linux")]
use std::env;
#[cfg(target_os = "linux")]
use std::ffi::CString;

use crossbeam_channel::unbounded;
use log::error;
use polly::event_manager::EventManager;
use vmm::resources::VmResources;
use vmm::vmm_config::kernel_bundle::KernelBundle;
use vmm::vmm_config::kernel_cmdline::KernelCmdlineConfig;
use vmm::vmm_config::vsock::VsockDeviceConfig;

use super::error::{BuildError, Error, Result, RuntimeError};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

const DEFAULT_KERNEL_CMDLINE: &str =
    "reboot=k panic=-1 panic_print=0 nomodule console=hvc0 quiet 8250.nr_uarts=0";
const INIT_PATH: &str = "/init.krun";

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Handle to a configured VM ready to run.
pub struct Vm {
    vmr: VmResources,
    exec_path: Option<String>,
    args: Option<String>,
    env: Option<String>,
    workdir: Option<String>,
    rlimits: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl Vm {
    /// Create a new Vm instance.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        vmr: VmResources,
        exec_path: Option<String>,
        args: Option<String>,
        env: Option<String>,
        workdir: Option<String>,
        rlimits: Option<String>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Self {
        Self {
            vmr,
            exec_path,
            args,
            env,
            workdir,
            rlimits,
            uid,
            gid,
        }
    }

    /// Run the VM.
    ///
    /// This method blocks until the VM exits. Returns the exit code on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The kernel cannot be loaded
    /// - The VM fails to build
    /// - The event loop encounters an error
    pub fn run(mut self) -> Result<i32> {
        // Set process name on Linux
        #[cfg(target_os = "linux")]
        {
            let prname = match env::var("HOSTNAME") {
                Ok(val) => CString::new(format!("VM:{val}")).unwrap_or_default(),
                Err(_) => CString::new("libkrun VM").unwrap_or_default(),
            };
            unsafe { libc::prctl(libc::PR_SET_NAME, prname.as_ptr()) };
        }

        // Create event manager
        let mut event_manager = EventManager::new()
            .map_err(|e| Error::Build(BuildError::Start(format!("EventManager: {e:?}"))))?;

        // Load kernel from libkrunfw if not already configured
        if self.vmr.external_kernel.is_none()
            && self.vmr.kernel_bundle.is_none()
            && self.vmr.firmware_config.is_none()
            && cfg!(not(feature = "efi"))
        {
            self.load_krunfw()?;
        }

        // Build kernel command line
        let kernel_cmdline = KernelCmdlineConfig {
            prolog: Some(format!("{DEFAULT_KERNEL_CMDLINE} init={INIT_PATH}")),
            krun_env: Some(format!(
                " {} {} {} {}",
                self.get_exec_path(),
                self.get_workdir(),
                self.get_rlimits(),
                self.get_env(),
            )),
            epilog: Some(format!(" -- {}", self.get_args())),
        };

        self.vmr
            .set_kernel_cmdline(kernel_cmdline)
            .map_err(|e| Error::Build(BuildError::Start(format!("kernel cmdline: {e:?}"))))?;

        // Configure vsock
        self.configure_vsock()?;

        // Set UID/GID if specified
        self.set_credentials()?;

        // Build the microVM
        let (sender, _receiver) = unbounded();

        let _vmm = vmm::builder::build_microvm(&self.vmr, &mut event_manager, None, sender)
            .map_err(|e| Error::Build(BuildError::Start(format!("build_microvm: {e:?}"))))?;

        // Start worker threads if needed
        #[cfg(target_os = "macos")]
        if self.vmr.gpu_virgl_flags.is_some() {
            vmm::worker::start_worker_thread(_vmm.clone(), _receiver)
                .map_err(|e| Error::Runtime(RuntimeError::EventLoop(format!("{e:?}"))))?;
        }

        #[cfg(target_arch = "x86_64")]
        if self.vmr.split_irqchip {
            vmm::worker::start_worker_thread(_vmm.clone(), _receiver.clone())
                .map_err(|e| Error::Runtime(RuntimeError::EventLoop(format!("{e:?}"))))?;
        }

        #[cfg(any(feature = "amd-sev", feature = "tdx"))]
        vmm::worker::start_worker_thread(_vmm.clone(), _receiver.clone())
            .map_err(|e| Error::Runtime(RuntimeError::EventLoop(format!("{e:?}"))))?;

        // Run the event loop
        loop {
            match event_manager.run() {
                Ok(_) => {}
                Err(e) => {
                    error!("Error in EventManager loop: {e:?}");
                    return Err(Error::Runtime(RuntimeError::EventLoop(format!("{e:?}"))));
                }
            }
        }
    }

    /// Load kernel from libkrunfw.
    fn load_krunfw(&mut self) -> Result<()> {
        // Try to load libkrunfw
        let krunfw = load_krunfw_library()?;

        // Get kernel from libkrunfw
        let mut kernel_guest_addr: u64 = 0;
        let mut kernel_entry_addr: u64 = 0;
        let mut kernel_size: usize = 0;

        let kernel_host_addr = unsafe {
            (krunfw.get_kernel)(
                &mut kernel_guest_addr as *mut u64,
                &mut kernel_entry_addr as *mut u64,
                &mut kernel_size as *mut usize,
            )
        };

        let kernel_bundle = KernelBundle {
            host_addr: kernel_host_addr as u64,
            guest_addr: kernel_guest_addr,
            entry_addr: kernel_entry_addr,
            size: kernel_size,
        };

        self.vmr
            .set_kernel_bundle(kernel_bundle)
            .map_err(|e| Error::Build(BuildError::Krunfw(format!("{e:?}"))))?;

        Ok(())
    }

    /// Configure vsock device.
    fn configure_vsock(&mut self) -> Result<()> {
        use devices::virtio::TsiFlags;

        let mut tsi_flags = TsiFlags::empty();

        // Enable TSI if no virtio-net configured
        #[cfg(feature = "net")]
        if self.vmr.net.list.is_empty() {
            tsi_flags |= TsiFlags::HIJACK_INET;
        }

        #[cfg(not(feature = "net"))]
        {
            tsi_flags |= TsiFlags::HIJACK_INET;
        }

        // Enable TSI for AF_UNIX if single root virtio-fs
        #[cfg(not(feature = "tee"))]
        if tsi_flags.contains(TsiFlags::HIJACK_INET)
            && self.vmr.fs.len() == 1
            && self.vmr.fs[0].shared_dir == "/"
        {
            tsi_flags |= TsiFlags::HIJACK_UNIX;
        }

        let vsock_config = VsockDeviceConfig {
            vsock_id: "vsock0".to_string(),
            guest_cid: 3,
            host_port_map: None,
            unix_ipc_port_map: None,
            tsi_flags,
        };

        self.vmr
            .set_vsock_device(vsock_config)
            .map_err(|e| Error::Build(BuildError::DeviceRegistration(format!("vsock: {e:?}"))))?;

        Ok(())
    }

    /// Set UID/GID credentials.
    fn set_credentials(&self) -> Result<()> {
        if let Some(gid) = self.gid {
            if unsafe { libc::setgid(gid) } != 0 {
                error!("Failed to set gid {gid}");
                return Err(Error::Runtime(RuntimeError::Shutdown(format!(
                    "setgid({gid}) failed"
                ))));
            }
        }

        if let Some(uid) = self.uid {
            if unsafe { libc::setuid(uid) } != 0 {
                error!("Failed to set uid {uid}");
                return Err(Error::Runtime(RuntimeError::Shutdown(format!(
                    "setuid({uid}) failed"
                ))));
            }
        }

        Ok(())
    }

    fn get_exec_path(&self) -> String {
        self.exec_path
            .as_ref()
            .map(|p| format!("KRUN_INIT={p}"))
            .unwrap_or_default()
    }

    fn get_workdir(&self) -> String {
        self.workdir
            .as_ref()
            .map(|p| format!("KRUN_WORKDIR={p}"))
            .unwrap_or_default()
    }

    fn get_rlimits(&self) -> String {
        self.rlimits
            .as_ref()
            .map(|r| format!("KRUN_RLIMITS={r}"))
            .unwrap_or_default()
    }

    fn get_env(&self) -> String {
        self.env
            .as_ref()
            .map(|e| format!("KRUN_ENV={e}"))
            .unwrap_or_default()
    }

    fn get_args(&self) -> String {
        self.args.clone().unwrap_or_default()
    }
}

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// Bindings to libkrunfw functions.
struct KrunfwBindings {
    get_kernel: unsafe extern "C" fn(*mut u64, *mut u64, *mut usize) -> *mut std::ffi::c_char,
    #[allow(dead_code)]
    _library: libloading::Library,
}

/// Library name for libkrunfw.
#[cfg(target_os = "linux")]
const KRUNFW_NAME: &str = "libkrunfw.so.5";
#[cfg(target_os = "macos")]
const KRUNFW_NAME: &str = "libkrunfw.5.dylib";

/// Load the libkrunfw library.
fn load_krunfw_library() -> Result<KrunfwBindings> {
    let library = unsafe { libloading::Library::new(KRUNFW_NAME) }
        .map_err(|e| Error::Build(BuildError::Krunfw(format!("load {KRUNFW_NAME}: {e}"))))?;

    let get_kernel = unsafe {
        *library
            .get::<unsafe extern "C" fn(*mut u64, *mut u64, *mut usize) -> *mut std::ffi::c_char>(
                b"krunfw_get_kernel\0",
            )
            .map_err(|e| Error::Build(BuildError::Krunfw(format!("krunfw_get_kernel: {e}"))))?
    };

    Ok(KrunfwBindings {
        get_kernel,
        _library: library,
    })
}
