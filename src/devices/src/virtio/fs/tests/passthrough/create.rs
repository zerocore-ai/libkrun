use std::{ffi::CString, fs, io};

use crate::virtio::{
    bindings,
    fs::filesystem::{Context, Extensions, FileSystem},
    fuse::FsOptions,
};

use super::helper;

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[test]
fn test_mkdir_basic() -> io::Result<()> {
    // Create test directory with a file
    let files = vec![("existing_file", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a new directory
    let dir_name = CString::new("new_dir").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let entry = fs.mkdir(ctx, 1, &dir_name, 0o755, 0, Extensions::default())?;

    // Verify the directory was created with correct mode
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755);

    // Verify we can look it up
    let lookup_entry = fs.lookup(ctx, 1, &dir_name)?;
    assert_eq!(lookup_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    assert_eq!(lookup_entry.inode, entry.inode);

    // Verify the directory exists on disk
    let dir_path = temp_dir.path().join("new_dir");
    assert!(dir_path.exists());
    assert!(dir_path.is_dir());

    // Verify override xattr was set with context uid/gid
    let xattr_value = helper::get_xattr(&dir_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set");

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "1000"); // Context default uid
    assert_eq!(parts[1], "1000"); // Context default gid
    assert_eq!(parts[2], "040755");  // full mode in octal (S_IFDIR | 0755)

    Ok(())
}

#[test]
fn test_mkdir_with_context() -> io::Result<()> {
    // Create test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create directory with custom context
    let dir_name = CString::new("custom_dir").unwrap();
    let ctx = Context {
        uid: 2500,
        gid: 2500,
        pid: 5678,
    };
    let entry = fs.mkdir(ctx, 1, &dir_name, 0o700, 0, Extensions::default())?;

    // Verify the directory has correct permissions
    assert_eq!(entry.attr.st_mode & 0o777, 0o700);

    // Verify override xattr has custom uid/gid
    let dir_path = temp_dir.path().join("custom_dir");
    let xattr_value = helper::get_xattr(&dir_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some());

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts[0], "2500"); // Custom uid
    assert_eq!(parts[1], "2500"); // Custom gid
    assert_eq!(parts[2], "040700");  // full mode in octal (S_IFDIR | 0700)

    // Verify getattr returns the overridden values
    let (attr, _) = fs.getattr(ctx, entry.inode, None)?;
    assert_eq!(attr.st_uid, 2500);
    assert_eq!(attr.st_gid, 2500);
    assert_eq!(attr.st_mode & 0o777, 0o700);

    Ok(())
}

#[test]
fn test_create_file_basic() -> io::Result<()> {
    // Create test directory
    let files = vec![("dir1", true, 0o755)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a new file in root
    let file_name = CString::new("new_file.txt").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let (entry, handle, _opts) = fs.create(
        ctx,
        1,
        &file_name,
        0o644,
        bindings::LINUX_O_CREAT as u32,
        0o022,
        Extensions::default(),
    )?;

    // Verify the file was created with correct mode
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);
    assert_eq!(entry.attr.st_mode & 0o777, 0o644);
    assert!(handle.is_some());

    // Verify the file exists on disk
    let file_path = temp_dir.path().join("new_file.txt");
    assert!(file_path.exists());
    assert!(file_path.is_file());

    // Verify override xattr was set
    let xattr_value = helper::get_xattr(&file_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set");

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts[0], "1000"); // Context default uid
    assert_eq!(parts[1], "1000"); // Context default gid
    assert_eq!(parts[2], "0100644");  // full mode in octal (S_IFREG | 0644)

    // Release the handle
    if let Some(h) = handle {
        fs.release(ctx, entry.inode, 0, h, false, false, None)?;
    }

    Ok(())
}

#[test]
fn test_create_file_with_umask() -> io::Result<()> {
    // Create test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a file with umask applied
    let file_name = CString::new("umask_file").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let (entry, handle, _opts) = fs.create(
        ctx,
        1,
        &file_name,
        0o777, // Request all permissions
        bindings::LINUX_O_CREAT as u32,
        0o027, // umask removes group write and all other permissions
        Extensions::default(),
    )?;

    // Verify the file mode after umask
    assert_eq!(entry.attr.st_mode & 0o777, 0o750); // 0777 & ~0027 = 0750

    // Verify xattr reflects the actual mode
    let file_path = temp_dir.path().join("umask_file");
    let xattr_value = helper::get_xattr(&file_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some());

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts[2], "0100750"); // full mode in octal (S_IFREG | 0750)

    // Release the handle
    if let Some(h) = handle {
        fs.release(ctx, entry.inode, 0, h, false, false, None)?;
    }

    Ok(())
}

#[test]
fn test_mknod_basic() -> io::Result<()> {
    // Create test directory
    let files = vec![];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a FIFO (named pipe)
    let fifo_name = CString::new("test_fifo").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let entry = fs.mknod(
        ctx,
        1,
        &fifo_name,
        mode_cast!(libc::S_IFIFO | 0o660),
        0,
        0,
        Extensions::default(),
    )?;

    // Verify the FIFO was created
    assert_eq!(entry.attr.st_mode as u32 & mode_cast!(libc::S_IFMT), mode_cast!(libc::S_IFIFO));
    assert_eq!(entry.attr.st_mode & 0o777, 0o660);

    // Verify the file exists on disk
    let fifo_path = temp_dir.path().join("test_fifo");
    assert!(fifo_path.exists());
    let metadata = fs::metadata(&fifo_path)?;

    // Check that the file on disk is actually a regular file (not a special file)
    // since we now create special files as regular files to support xattr
    assert!(metadata.file_type().is_file(), "Special files should be stored as regular files");

    // Verify xattr was set correctly with the full mode (including file type)
    let xattr_value = helper::get_xattr(&fifo_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some(), "Override xattr should be set on special files");

    // Parse the xattr to verify it contains the correct file type
    if let Some(xattr) = xattr_value {
        let parts: Vec<&str> = xattr.split(':').collect();
        assert_eq!(parts.len(), 3, "xattr should have format uid:gid:mode");
        let stored_mode = u32::from_str_radix(parts[2], 8).expect("mode should be valid octal");
        assert_eq!(stored_mode & mode_cast!(libc::S_IFMT), mode_cast!(libc::S_IFIFO),
            "xattr should store the correct file type");
    }

    Ok(())
}

#[test]
fn test_symlink_basic() -> io::Result<()> {
    // Create test directory with a target file
    let files = vec![("target_file", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a symlink
    let link_name = CString::new("symlink").unwrap();
    let target_name = CString::new("target_file").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let entry = fs.symlink(ctx, &target_name, 1, &link_name, Extensions::default())?;

    // Verify the symlink was created
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK);

    // Verify the symlink exists on disk
    let link_path = temp_dir.path().join("symlink");
    assert!(link_path.exists());
    let metadata = fs::symlink_metadata(&link_path)?;
    assert!(metadata.file_type().is_symlink());

    // Verify the symlink points to the correct target
    let target = fs::read_link(&link_path)?;
    assert_eq!(target.to_str().unwrap(), "target_file");

    // Check if xattr was set (some filesystems don't support xattrs on symlinks)
    let xattr_value = helper::get_xattr(&link_path, "user.containers.override_stat")?;
    if xattr_value.is_some() {
        let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
        assert_eq!(parts[0], "1000"); // Context default uid
        assert_eq!(parts[1], "1000"); // Context default gid
    }

    Ok(())
}

#[test]
fn test_create_nested() -> io::Result<()> {
    // Create test directory structure
    let files = vec![
        ("parent", true, 0o755),
        ("parent/child", true, 0o755),
    ];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context {
        uid: 3000,
        gid: 3000,
        pid: 9999,
    };

    // Look up parent directory
    let parent_name = CString::new("parent").unwrap();
    let parent_entry = fs.lookup(ctx, 1, &parent_name)?;

    // Create a file in parent directory
    let file_name = CString::new("nested_file").unwrap();
    let (file_entry, handle, _opts) = fs.create(
        ctx,
        parent_entry.inode,
        &file_name,
        0o666,
        bindings::LINUX_O_CREAT as u32,
        0o022,
        Extensions::default(),
    )?;

    assert_eq!(file_entry.attr.st_mode & 0o777, 0o644);

    // Verify xattr has custom context uid/gid
    let file_path = temp_dir.path().join("parent/nested_file");
    let xattr_value = helper::get_xattr(&file_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some());

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts[0], "3000"); // Custom uid
    assert_eq!(parts[1], "3000"); // Custom gid

    // Look up child directory
    let child_name = CString::new("child").unwrap();
    let child_entry = fs.lookup(ctx, parent_entry.inode, &child_name)?;

    // Create a directory in child
    let dir_name = CString::new("nested_dir").unwrap();
    let dir_entry = fs.mkdir(
        ctx,
        child_entry.inode,
        &dir_name,
        0o777,
        0o022,
        Extensions::default(),
    )?;

    assert_eq!(dir_entry.attr.st_mode & 0o777, 0o755);

    // Verify nested directory xattr
    let dir_path = temp_dir.path().join("parent/child/nested_dir");
    let xattr_value = helper::get_xattr(&dir_path, "user.containers.override_stat")?;
    assert!(xattr_value.is_some());

    let xattr_str = xattr_value.unwrap();
    let parts: Vec<&str> = xattr_str.split(':').collect();
    assert_eq!(parts[0], "3000"); // Custom uid
    assert_eq!(parts[1], "3000"); // Custom gid

    // Release the handle
    if let Some(h) = handle {
        fs.release(ctx, file_entry.inode, 0, h, false, false, None)?;
    }

    Ok(())
}

#[test]
fn test_create_duplicate_name() -> io::Result<()> {
    // Create test directory with existing file
    let files = vec![("existing", false, 0o644)];

    let (fs, temp_dir) = helper::create_passthroughfs(files)?;
    helper::debug_print_dir(&temp_dir, true)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Try to create a file with same name
    let file_name = CString::new("existing").unwrap();
    let ctx = Context {
        uid: 1000,
        gid: 1000,
        pid: 1234,
    };
    let result = fs.create(
        ctx,
        1,
        &file_name,
        0o644,
        bindings::LINUX_O_CREAT as u32 | bindings::LINUX_O_EXCL as u32, // O_EXCL should fail if exists
        0,
        Extensions::default(),
    );

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EEXIST));

    // Try to create a directory with same name as file
    let result = fs.mkdir(ctx, 1, &file_name, 0o755, 0, Extensions::default());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EEXIST));

    Ok(())
}
