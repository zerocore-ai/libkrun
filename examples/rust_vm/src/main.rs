//! Simple example demonstrating the krun-api Rust API.
//!
//! Prerequisites:
//! - libkrunfw installed (provides the kernel)
//! - A rootfs with /init.krun or specify your own executable

use msb_krun::{Result, VmBuilder};

fn main() -> Result<()> {
    // Create a simple VM that runs /bin/sh
    let builder = VmBuilder::new()
        .machine(|m| m.vcpus(2).memory_mib(1024));

    #[cfg(not(feature = "tee"))]
    let builder = builder.fs(|fs| fs.root("/")); // Share host root as guest root

    let exit_code = builder
        .exec(|e| {
            e.path("/bin/echo")
                .args(["Hello from libkrun VM!"])
                .env("HOME", "/root")
        })
        .build()?
        .run()?;

    println!("VM exited with code: {}", exit_code);
    Ok(())
}
