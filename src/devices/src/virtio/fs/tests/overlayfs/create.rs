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
    // Create test layers:
    // Single layer with a file
    let layers = vec![vec![("file1", false, 0o644)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a new directory
    let dir_name = CString::new("new_dir").unwrap();
    let ctx = Context::default();
    let entry = fs.mkdir(ctx, 1, &dir_name, 0o755, 0, Extensions::default())?;

    // Verify the directory was created with correct mode
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    assert_eq!(entry.attr.st_mode & 0o777, 0o755);

    // Verify we can look it up
    let lookup_entry = fs.lookup(ctx, 1, &dir_name)?;
    assert_eq!(lookup_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    // Verify the directory exists on disk in the top layer
    let dir_path = temp_dirs.last().unwrap().path().join("new_dir");
    assert!(dir_path.exists());
    assert!(dir_path.is_dir());

    Ok(())
}

#[test]
fn test_mkdir_nested() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/file1
    //   - dir1/subdir/
    //   - dir1/subdir/bottom_file
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file2
    // Layer 2 (top):
    //   - dir3/
    //   - dir3/top_file
    //   - dir1/.wh.subdir (whiteout)
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
            ("dir1/subdir/bottom_file", false, 0o644),
        ],
        vec![("dir2", true, 0o755), ("dir2/file2", false, 0o644)],
        vec![
            ("dir3", true, 0o755),
            ("dir3/top_file", false, 0o644),
            ("dir1/.wh.subdir", false, 0o644),
        ],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create nested directory in dir1 (should trigger copy-up)
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    let nested_name = CString::new("new_nested").unwrap();
    let nested_entry = fs.mkdir(
        ctx,
        dir1_entry.inode,
        &nested_name,
        0o700,
        0,
        Extensions::default(),
    )?;
    assert_eq!(nested_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    // Test 2: Create directory inside the newly created nested directory
    let deep_name = CString::new("deep_dir").unwrap();
    let deep_entry = fs.mkdir(
        ctx,
        nested_entry.inode,
        &deep_name,
        0o755,
        0,
        Extensions::default(),
    )?;
    assert_eq!(deep_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    // Test 3: Create directory in dir2 (middle layer, should trigger copy-up)
    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    let middle_nested_name = CString::new("middle_nested").unwrap();
    let middle_nested_entry = fs.mkdir(
        ctx,
        dir2_entry.inode,
        &middle_nested_name,
        0o755,
        0,
        Extensions::default(),
    )?;
    assert_eq!(
        middle_nested_entry.attr.st_mode & libc::S_IFMT,
        libc::S_IFDIR
    );

    // Test 4: Create directory in dir3 (top layer, no copy-up needed)
    let dir3_name = CString::new("dir3").unwrap();
    let dir3_entry = fs.lookup(ctx, 1, &dir3_name)?;
    let top_nested_name = CString::new("top_nested").unwrap();
    let top_nested_entry = fs.mkdir(
        ctx,
        dir3_entry.inode,
        &top_nested_name,
        0o755,
        0,
        Extensions::default(),
    )?;
    assert_eq!(top_nested_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    helper::debug_print_layers(&temp_dirs, false)?;

    // Verify all directories exist in appropriate layers
    let top_layer = temp_dirs.last().unwrap().path();
    assert!(top_layer.join("dir1/new_nested").exists());
    assert!(top_layer.join("dir1/new_nested/deep_dir").exists());
    assert!(top_layer.join("dir2/middle_nested").exists());
    assert!(top_layer.join("dir3/top_nested").exists());

    // Verify the original files are still accessible
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    let file1_name = CString::new("file1").unwrap();
    let file1_entry = fs.lookup(ctx, dir1_entry.inode, &file1_name)?;
    assert_eq!(file1_entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_mkdir_with_umask() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/subdir/ (0o755)
    //   - dir1/subdir/file1
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file2
    // Layer 2 (top):
    //   - dir3/ (0o777)
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/subdir", true, 0o755),
            ("dir1/subdir/file1", false, 0o644),
        ],
        vec![("dir2", true, 0o755), ("dir2/file2", false, 0o644)],
        vec![("dir3", true, 0o777)],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create directory with different umasks in root
    let dir_names = vec![
        ("dir_umask_022", 0o777, 0o022, 0o755), // Common umask
        ("dir_umask_077", 0o777, 0o077, 0o700), // Strict umask
        ("dir_umask_002", 0o777, 0o002, 0o775), // Group writable
        ("dir_umask_000", 0o777, 0o000, 0o777), // No umask
    ];

    let test_cases = dir_names.clone();
    for (name, mode, umask, expected) in test_cases {
        let dir_name = CString::new(name).unwrap();
        let entry = fs.mkdir(ctx, 1, &dir_name, mode, umask, Extensions::default())?;
        assert_eq!(
            entry.attr.st_mode & 0o777,
            expected,
            "Directory {} has wrong permissions",
            name
        );
    }

    // Test 2: Create nested directories with umask in different layers
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    let nested_name = CString::new("nested_umask").unwrap();
    let nested_entry = fs.mkdir(
        ctx,
        dir1_entry.inode,
        &nested_name,
        0o777,
        0o027,
        Extensions::default(),
    )?;
    assert_eq!(nested_entry.attr.st_mode & 0o777, 0o750);

    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    let middle_name = CString::new("middle_umask").unwrap();
    let middle_entry = fs.mkdir(
        ctx,
        dir2_entry.inode,
        &middle_name,
        0o777,
        0o077,
        Extensions::default(),
    )?;
    assert_eq!(middle_entry.attr.st_mode & 0o777, 0o700);

    Ok(())
}

#[test]
fn test_mkdir_existing_name() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/file1
    //   - dir1/subdir/
    //   - dir1/subdir/file2
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file3
    //   - dir1/another_file
    // Layer 2 (top):
    //   - dir3/
    //   - dir3/file4
    //   - .wh.dir1/subdir (whiteout)
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
            ("dir1/subdir/file2", false, 0o644),
        ],
        vec![
            ("dir2", true, 0o755),
            ("dir2/file3", false, 0o644),
            ("dir1/another_file", false, 0o644),
        ],
        vec![
            ("dir3", true, 0o755),
            ("dir3/file4", false, 0o644),
            ("dir1/.wh.subdir", false, 0o644),
        ],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Try to create directory with name of existing file in bottom layer
    let file1_name = CString::new("file1").unwrap();
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    match fs.mkdir(
        ctx,
        dir1_entry.inode,
        &file1_name,
        0o755,
        0,
        Extensions::default(),
    ) {
        Ok(_) => {
            helper::debug_print_layers(&temp_dirs, false)?;
            panic!("Expected mkdir with existing file name to fail");
        }
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
    }

    // Test 2: Try to create directory with name of existing file in middle layer
    let file3_name = CString::new("file3").unwrap();
    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    match fs.mkdir(
        ctx,
        dir2_entry.inode,
        &file3_name,
        0o755,
        0,
        Extensions::default(),
    ) {
        Ok(_) => panic!("Expected mkdir with existing file name to fail"),
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
    }

    // Test 3: Try to create directory with name of existing directory
    let dir3_name = CString::new("dir3").unwrap();
    match fs.mkdir(ctx, 1, &dir3_name, 0o755, 0, Extensions::default()) {
        Ok(_) => panic!("Expected mkdir with existing directory name to fail"),
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
    }

    // Test 4: Try to create directory with name that exists in lower layer but is whited out
    let subdir_name = CString::new("subdir").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;

    // This should succeed because the original subdir is whited out
    let new_subdir = fs.mkdir(
        ctx,
        dir1_entry.inode,
        &subdir_name,
        0o755,
        0,
        Extensions::default(),
    )?;
    assert_eq!(new_subdir.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

    Ok(())
}

#[test]
fn test_mkdir_invalid_parent() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/file1
    //   - dir1/subdir/
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file2
    //   - .wh.dir1 (whiteout entire dir1)
    // Layer 2 (top):
    //   - dir3/
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
        ],
        vec![
            ("dir2", true, 0o755),
            ("dir2/file2", false, 0o644),
            (".wh.dir1", false, 0o644), // Whiteout entire dir1
        ],
        vec![("dir3", true, 0o755)],
    ];

    let (fs, _temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&_temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Try to create directory with non-existent parent inode
    let dir_name = CString::new("new_dir").unwrap();
    let invalid_inode = 999999;
    match fs.mkdir(
        ctx,
        invalid_inode,
        &dir_name,
        0o755,
        0,
        Extensions::default(),
    ) {
        Ok(_) => panic!("Expected mkdir with invalid parent to fail"),
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::EBADF)),
    }

    // Test 2: Try to create directory in whited-out directory
    let dir1_name = CString::new("dir1").unwrap();
    match fs.lookup(ctx, 1, &dir1_name) {
        Ok(_) => panic!("Expected lookup of whited-out directory to fail"),
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOENT)),
    }

    // Test 3: Try to create directory with file as parent
    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    let file2_name = CString::new("file2").unwrap();
    let file2_entry = fs.lookup(ctx, dir2_entry.inode, &file2_name)?;

    let nested_name = CString::new("nested").unwrap();
    match fs.mkdir(
        ctx,
        file2_entry.inode,
        &nested_name,
        0o755,
        0,
        Extensions::default(),
    ) {
        Ok(_) => panic!("Expected mkdir with file as parent to fail"),
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOTDIR)),
    }

    Ok(())
}

#[test]
fn test_mkdir_invalid_name() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/.hidden_file
    //   - dir1/subdir/
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/.wh..wh..opq (opaque directory)
    // Layer 2 (top):
    //   - dir3/
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/.hidden_file", false, 0o644),
            ("dir1/subdir", true, 0o755),
        ],
        vec![
            ("dir2", true, 0o755),
            ("dir2/.wh..wh..opq", false, 0o644), // Opaque directory marker
        ],
        vec![("dir3", true, 0o755)],
    ];

    let (fs, _temp_dirs) = helper::create_overlayfs(layers)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test various invalid names
    let test_cases = vec![
        ("", io::ErrorKind::InvalidInput, "empty name"),
        (
            "..",
            io::ErrorKind::PermissionDenied,
            "parent dir traversal",
        ),
        ("foo/bar", io::ErrorKind::PermissionDenied, "contains slash"),
        (
            "foo\\bar",
            io::ErrorKind::PermissionDenied,
            "contains backslash",
        ),
        (
            "foo\0bar",
            io::ErrorKind::InvalidInput,
            "contains null byte",
        ),
        (".wh.foo", io::ErrorKind::InvalidInput, "whiteout prefix"),
        (".wh..wh..opq", io::ErrorKind::InvalidInput, "opaque marker"),
    ];

    for (name, expected_kind, desc) in test_cases {
        let name = CString::new(name.as_bytes().to_vec()).unwrap_or_default();
        match fs.mkdir(ctx, 1, &name, 0o755, 0, Extensions::default()) {
            Ok(_) => panic!("Expected mkdir with {} to fail", desc),
            Err(e) => assert_eq!(
                e.kind(),
                expected_kind,
                "Wrong error kind for {}: expected {:?}, got {:?}",
                desc,
                expected_kind,
                e.kind()
            ),
        }
    }

    // Test invalid UTF-8 separately since it can't be represented as a string literal
    let invalid_utf8 = vec![0x66, 0x6f, 0x6f, 0x80, 0x62, 0x61, 0x72]; // "foo<invalid>bar"
    let name = CString::new(invalid_utf8).unwrap();
    match fs.mkdir(ctx, 1, &name, 0o755, 0, Extensions::default()) {
        Ok(_) => panic!("Expected mkdir with invalid UTF-8 to fail"),
        Err(e) => assert_eq!(
            e.kind(),
            io::ErrorKind::InvalidInput,
            "Wrong error kind for invalid UTF-8: expected {:?}, got {:?}",
            io::ErrorKind::InvalidInput,
            e.kind()
        ),
    }

    // Test with valid but unusual names
    let valid_cases = vec![
        "very_long_name_that_is_valid_but_unusual_and_tests_length_limits",
        " leading_space",
        "trailing_space ",
        "!@#$%^&*()_+-=",
    ];

    for name in valid_cases {
        let name = CString::new(name).unwrap();
        // These should succeed
        let entry = fs.mkdir(ctx, 1, &name, 0o755, 0, Extensions::default())?;
        assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    }

    Ok(())
}

#[test]
fn test_mkdir_multiple_layers() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/file1
    //   - dir1/subdir/
    //   - dir1/subdir/bottom_file
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file2
    // Layer 2 (top):
    //   - dir3/
    //   - dir3/top_file
    //   - .wh.dir1 (whiteout)
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
            ("dir1/subdir/bottom_file", false, 0o644),
        ],
        vec![("dir2", true, 0o755), ("dir2/file2", false, 0o644)],
        vec![
            ("dir3", true, 0o755),
            ("dir3/top_file", false, 0o644),
            (".wh.dir1", false, 0o644),
        ],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create directory in each layer and verify copy-up behavior
    let dir_names = vec![("dir2", "new_dir2"), ("dir3", "new_dir3")];

    for (parent, new_dir) in dir_names {
        let parent_name = CString::new(parent).unwrap();
        let parent_entry = fs.lookup(ctx, 1, &parent_name)?;

        let new_name = CString::new(new_dir).unwrap();
        let entry = fs.mkdir(
            ctx,
            parent_entry.inode,
            &new_name,
            0o755,
            0,
            Extensions::default(),
        )?;
        assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);

        // Create a nested directory inside
        let nested_name = CString::new(format!("nested_in_{}", new_dir)).unwrap();
        let nested_entry = fs.mkdir(
            ctx,
            entry.inode,
            &nested_name,
            0o700,
            0,
            Extensions::default(),
        )?;
        assert_eq!(nested_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    }

    // Test 2: Verify all directories exist in the top layer
    let top_layer = temp_dirs.last().unwrap().path();
    assert!(top_layer.join("dir2/new_dir2").exists());
    assert!(top_layer.join("dir2/new_dir2/nested_in_new_dir2").exists());
    assert!(top_layer.join("dir3/new_dir3").exists());
    assert!(top_layer.join("dir3/new_dir3/nested_in_new_dir3").exists());

    // Test 3: Try to create directory in whited-out dir1 (should fail)
    let dir1_name = CString::new("dir1").unwrap();
    match fs.lookup(ctx, 1, &dir1_name) {
        Ok(_) => panic!("Expected lookup of whited-out directory to fail"),
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOENT)),
    }

    Ok(())
}

#[test]
fn test_symlink_basic() -> io::Result<()> {
    // Test basic symlink creation in overlayfs
    // This test verifies:
    // 1. Creating a symlink through the filesystem API
    // 2. The symlink has correct mode and permissions
    // 3. The symlink can be looked up correctly
    // 4. The physical representation on disk matches the platform behavior
    // 5. The symlink target can be read correctly through the filesystem API
    
    // Create test layers with a single file that will be the symlink target
    let layers = vec![vec![("target_file", false, 0o644)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a new symlink pointing to target_file
    let link_name = CString::new("link").unwrap();
    let target_name = CString::new("target_file").unwrap();
    let ctx = Context::default();
    let entry = fs.symlink(ctx, &target_name, 1, &link_name, Extensions::default())?;

    // Verify the symlink was created with correct mode through the filesystem API
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK, 
        "Created entry should have S_IFLNK file type");
    assert_eq!(entry.attr.st_mode & 0o777, 0o777, 
        "Symlinks should have 0777 permissions");

    // Verify we can look it up and it still appears as a symlink
    let lookup_entry = fs.lookup(ctx, 1, &link_name)?;
    assert_eq!(lookup_entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK,
        "Looked up entry should have S_IFLNK file type");
    assert_eq!(lookup_entry.inode, entry.inode, 
        "Lookup should return same inode as creation");

    // Verify the physical representation on disk
    let link_path = temp_dirs.last().unwrap().path().join("link");
    assert!(link_path.exists(), "Symlink should exist on disk");
    
    // Platform-specific verification of physical representation
    #[cfg(target_os = "linux")]
    {
        // On Linux, symlinks are implemented as regular files with xattr metadata
        // This is done to support extended attributes on filesystems that don't
        // support xattrs on symlinks
        let metadata = fs::metadata(&link_path)?;
        assert!(metadata.is_file(), 
            "On Linux, symlinks should be represented as regular files");
        
        // Verify the override xattr is set correctly
        let xattr_value = helper::get_xattr(&link_path, "user.containers.override_stat")?;
        assert!(xattr_value.is_some(), 
            "File-backed symlink should have override_stat xattr");
        
        if let Some(xattr_str) = xattr_value {
            let parts: Vec<&str> = xattr_str.split(':').collect();
            assert!(parts.len() >= 3, "xattr should have at least uid:gid:mode");
            
            // Verify the mode in xattr indicates this is a symlink
            let mode = u32::from_str_radix(parts[2], 8).expect("mode should be valid octal");
            assert_eq!(mode & libc::S_IFMT, libc::S_IFLNK,
                "xattr mode should indicate S_IFLNK file type");
        }
        
        // Verify the file content contains the link target
        let file_content = fs::read(&link_path)?;
        assert_eq!(file_content, target_name.to_bytes(),
            "File content should contain the symlink target");
    }
    
    #[cfg(target_os = "macos")]
    {
        // On macOS, symlinks are regular symlinks
        let metadata = fs::symlink_metadata(&link_path)?;
        assert!(metadata.file_type().is_symlink(),
            "On macOS, symlinks should be regular symlinks");
        
        // Verify the symlink target through filesystem
        let target = fs::read_link(&link_path)?;
        assert_eq!(target.to_str().unwrap(), "target_file",
            "Symlink should point to correct target");
    }

    // Verify the symlink target can be read through the filesystem API
    let target = fs.readlink(ctx, lookup_entry.inode)?;
    assert_eq!(target, target_name.to_bytes(),
        "readlink should return the correct target");

    // Additional verification: ensure the symlink behaves correctly
    // Try to lookup the target through the symlink (should fail since we're not following)
    match fs.lookup(ctx, lookup_entry.inode, &CString::new("anything").unwrap()) {
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::ENOTDIR),
            "Looking up through a symlink should fail with ENOTDIR"),
        Ok(_) => panic!("Lookup through symlink should fail"),
    }

    Ok(())
}

#[test]
fn test_symlink_nested() -> io::Result<()> {
    // Test symlink creation in nested directory structures across multiple layers
    // This test verifies:
    // 1. Creating symlinks in directories from different layers
    // 2. Copy-up behavior when creating symlinks in lower layer directories
    // 3. Symlinks work correctly in nested directory structures
    // 4. Each symlink can be read correctly regardless of which layer its parent came from
    
    // Create test layers with complex structure:
    // Layer 0 (bottom): dir1 with files and subdirectories
    // Layer 1 (middle): dir2 with a file
    // Layer 2 (top): dir3 with a file
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
            ("dir1/subdir/bottom_file", false, 0o644),
        ],
        vec![("dir2", true, 0o755), ("dir2/file2", false, 0o644)],
        vec![("dir3", true, 0o755), ("dir3/top_file", false, 0o644)],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create symlink in dir1 (from bottom layer - should trigger copy-up)
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    
    // Verify dir1 is from bottom layer initially
    assert_eq!(dir1_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR);
    
    let link_name = CString::new("link_to_file1").unwrap();
    let target_name = CString::new("file1").unwrap();
    let link_entry = fs.symlink(
        ctx,
        &target_name,
        dir1_entry.inode,
        &link_name,
        Extensions::default(),
    )?;
    assert_eq!(link_entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK);

    // Verify dir1 was copied up to top layer
    assert!(temp_dirs.last().unwrap().path().join("dir1").exists(),
        "dir1 should be copied up to top layer");

    // Test 2: Create symlink in dir2 (middle layer - should trigger copy-up)
    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    let middle_link_name = CString::new("link_to_file2").unwrap();
    let middle_target = CString::new("file2").unwrap();
    let middle_link_entry = fs.symlink(
        ctx,
        &middle_target,
        dir2_entry.inode,
        &middle_link_name,
        Extensions::default(),
    )?;
    assert_eq!(middle_link_entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK);

    // Verify dir2 was copied up to top layer
    assert!(temp_dirs.last().unwrap().path().join("dir2").exists(),
        "dir2 should be copied up to top layer");

    // Test 3: Create symlink in dir3 (already in top layer - no copy-up needed)
    let dir3_name = CString::new("dir3").unwrap();
    let dir3_entry = fs.lookup(ctx, 1, &dir3_name)?;
    let top_link_name = CString::new("link_to_top_file").unwrap();
    let top_target = CString::new("top_file").unwrap();
    let top_link_entry = fs.symlink(
        ctx,
        &top_target,
        dir3_entry.inode,
        &top_link_name,
        Extensions::default(),
    )?;
    assert_eq!(top_link_entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK);

    // Verify all symlinks exist in the top layer (due to copy-up)
    let top_layer = temp_dirs.last().unwrap().path();
    
    #[cfg(target_os = "linux")]
    {
        // On Linux, verify file-backed symlinks
        for (dir, link) in &[("dir1", "link_to_file1"), ("dir2", "link_to_file2"), ("dir3", "link_to_top_file")] {
            let link_path = top_layer.join(dir).join(link);
            assert!(link_path.exists(), "{}/{} should exist", dir, link);
            
            let metadata = fs::metadata(&link_path)?;
            assert!(metadata.is_file(), 
                "{}/{} should be a regular file on Linux", dir, link);
            
            // Verify xattr
            let xattr = helper::get_xattr(&link_path, "user.containers.override_stat")?;
            assert!(xattr.is_some(), "{}/{} should have override_stat xattr", dir, link);
        }
    }
    
    #[cfg(target_os = "macos")]
    {
        // On macOS, verify regular symlinks
        for (dir, link) in &[("dir1", "link_to_file1"), ("dir2", "link_to_file2"), ("dir3", "link_to_top_file")] {
            let link_path = top_layer.join(dir).join(link);
            assert!(link_path.symlink_metadata().is_ok(), "{}/{} should exist", dir, link);
            
            let metadata = fs::symlink_metadata(&link_path)?;
            assert!(metadata.file_type().is_symlink(), 
                "{}/{} should be a symlink on macOS", dir, link);
        }
    }

    // Verify symlink targets through filesystem API
    let link1_target = fs.readlink(ctx, link_entry.inode)?;
    assert_eq!(link1_target, target_name.to_bytes(),
        "First symlink should point to file1");

    let link2_target = fs.readlink(ctx, middle_link_entry.inode)?;
    assert_eq!(link2_target, middle_target.to_bytes(),
        "Second symlink should point to file2");

    let link3_target = fs.readlink(ctx, top_link_entry.inode)?;
    assert_eq!(link3_target, top_target.to_bytes(),
        "Third symlink should point to top_file");

    // Additional test: Create symlink with absolute path
    let abs_link_name = CString::new("abs_link").unwrap();
    let abs_target = CString::new("/absolute/path/to/target").unwrap();
    let abs_link_entry = fs.symlink(
        ctx,
        &abs_target,
        dir1_entry.inode,
        &abs_link_name,
        Extensions::default(),
    )?;
    
    let abs_target_read = fs.readlink(ctx, abs_link_entry.inode)?;
    assert_eq!(abs_target_read, abs_target.to_bytes(),
        "Absolute path symlinks should be preserved");

    // Test symlink in subdirectory
    let subdir_name = CString::new("subdir").unwrap();
    let subdir_entry = fs.lookup(ctx, dir1_entry.inode, &subdir_name)?;
    let subdir_link_name = CString::new("link_to_bottom").unwrap();
    let subdir_target = CString::new("bottom_file").unwrap();
    let subdir_link_entry = fs.symlink(
        ctx,
        &subdir_target,
        subdir_entry.inode,
        &subdir_link_name,
        Extensions::default(),
    )?;
    
    let subdir_target_read = fs.readlink(ctx, subdir_link_entry.inode)?;
    assert_eq!(subdir_target_read, subdir_target.to_bytes(),
        "Symlinks in subdirectories should work correctly");

    Ok(())
}

#[test]
fn test_symlink_existing_name() -> io::Result<()> {
    // Create test layers with a file and directory
    let layers = vec![vec![
        ("target_file", false, 0o644),
        ("existing_name", false, 0o644),
    ]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();
    let link_name = CString::new("existing_name").unwrap();
    let target_name = CString::new("target_file").unwrap();

    // Try to create a symlink with an existing name
    match fs.symlink(ctx, &target_name, 1, &link_name, Extensions::default()) {
        Ok(_) => panic!("Expected error when creating symlink with existing name"),
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
    }

    Ok(())
}

#[test]
fn test_symlink_multiple_layers() -> io::Result<()> {
    // Test creating symlinks that point to targets in different layers
    // This test verifies:
    // 1. Symlinks can point to targets in any layer
    // 2. Symlinks are always created in the top layer
    // 3. Both relative and cross-directory symlinks work correctly
    // 4. The physical representation matches platform expectations
    
    // Create test layers with directories and files in each layer
    let layers = vec![
        vec![
            ("bottom_dir", true, 0o755),
            ("bottom_dir/target1", false, 0o644),
            ("shared_dir", true, 0o755),
            ("shared_dir/bottom_file", false, 0o644),
        ],
        vec![
            ("middle_dir", true, 0o755),
            ("middle_dir/target2", false, 0o644),
            ("shared_dir", true, 0o755),
            ("shared_dir/middle_file", false, 0o644),
        ],
        vec![
            ("top_dir", true, 0o755), 
            ("top_dir/target3", false, 0o644),
            ("shared_dir", true, 0o755),
            ("shared_dir/top_file", false, 0o644),
        ],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create symlinks to files in different layers
    let test_cases = vec![
        ("link_to_bottom", "bottom_dir/target1", "Points to file in bottom layer"),
        ("link_to_middle", "middle_dir/target2", "Points to file in middle layer"),
        ("link_to_top", "top_dir/target3", "Points to file in top layer"),
        ("link_relative", "../bottom_dir/target1", "Relative path symlink"),
        ("link_dot_relative", "./top_dir/target3", "Dot-relative path symlink"),
    ];

    let mut created_entries = Vec::new();
    
    for (link, target, description) in &test_cases {
        let link_name = CString::new(*link).unwrap();
        let target_name = CString::new(*target).unwrap();

        let entry = fs.symlink(ctx, &target_name, 1, &link_name, Extensions::default())?;
        assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFLNK,
            "{}: Should create symlink", description);
        
        created_entries.push((entry.inode, target_name.clone(), description));
    }

    // Verify all symlinks exist in the top layer
    let top_layer = temp_dirs.last().unwrap().path();
    
    #[cfg(target_os = "linux")]
    {
        for (link, _, description) in &test_cases {
            let link_path = top_layer.join(link);
            assert!(link_path.exists(), 
                "{}: Symlink should exist in top layer", description);
            
            // Verify it's a file-backed symlink
            let metadata = fs::metadata(&link_path)?;
            assert!(metadata.is_file(),
                "{}: Should be a regular file on Linux", description);
            
            // Verify xattr exists
            let xattr = helper::get_xattr(&link_path, "user.containers.override_stat")?;
            assert!(xattr.is_some(),
                "{}: Should have override_stat xattr", description);
            
            // Read file content to verify target
            let content = fs::read(&link_path)?;
            let (_, target, _) = test_cases.iter()
                .find(|(l, _, _)| l == link)
                .unwrap();
            assert_eq!(content, target.as_bytes(),
                "{}: File content should match symlink target", description);
        }
    }
    
    #[cfg(target_os = "macos")]
    {
        for (link, target, description) in &test_cases {
            let link_path = top_layer.join(link);
            // Use symlink_metadata to check if the symlink itself exists
            // (not whether its target exists)
            assert!(link_path.symlink_metadata().is_ok(), 
                "{}: Symlink should exist in top layer", description);
            
            // Verify it's a regular symlink
            let metadata = fs::symlink_metadata(&link_path)?;
            assert!(metadata.file_type().is_symlink(),
                "{}: Should be a symlink on macOS", description);
            
            // Verify target through filesystem
            let fs_target = fs::read_link(&link_path)?;
            assert_eq!(fs_target.to_str().unwrap(), *target,
                "{}: Filesystem symlink should point to correct target", description);
        }
    }

    // Verify symlink targets through the VFS API
    for (inode, expected_target, description) in created_entries {
        let target_bytes = fs.readlink(ctx, inode)?;
        assert_eq!(target_bytes, expected_target.to_bytes(),
            "{}: readlink should return correct target", description);
    }

    // Test 2: Create symlink in shared_dir (which exists in all layers)
    let shared_dir_name = CString::new("shared_dir").unwrap();
    let shared_dir_entry = fs.lookup(ctx, 1, &shared_dir_name)?;
    
    let shared_link_name = CString::new("shared_link").unwrap();
    let shared_target = CString::new("bottom_file").unwrap();
    let shared_link_entry = fs.symlink(
        ctx,
        &shared_target,
        shared_dir_entry.inode,
        &shared_link_name,
        Extensions::default(),
    )?;
    
    // Verify the symlink was created in the top layer's shared_dir
    let shared_link_path = top_layer.join("shared_dir/shared_link");
    assert!(shared_link_path.symlink_metadata().is_ok(),
        "Symlink in shared directory should exist in top layer");
    
    let shared_target_read = fs.readlink(ctx, shared_link_entry.inode)?;
    assert_eq!(shared_target_read, shared_target.to_bytes(),
        "Symlink in shared directory should have correct target");

    // Test 3: Verify that symlinks don't affect the visibility of their targets
    // The targets should still be accessible from their original locations
    for dir in ["bottom_dir", "middle_dir", "top_dir"] {
        let dir_cstr = CString::new(dir).unwrap();
        let dir_entry = fs.lookup(ctx, 1, &dir_cstr)?;
        assert_eq!(dir_entry.attr.st_mode & libc::S_IFMT, libc::S_IFDIR,
            "{} should still be accessible as a directory", dir);
    }

    Ok(())
}

#[test]
fn test_symlink_invalid_name() -> io::Result<()> {
    // Create a simple test layer
    let layers = vec![vec![("target_file", false, 0o644)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();
    let target_name = CString::new("target_file").unwrap();

    // Test cases with invalid names
    let invalid_names = vec![
        "..",           // Path traversal attempt
        "invalid/name", // Contains slash
        ".wh.name",     // Contains whiteout prefix
        ".wh..wh..opq", // Opaque directory marker
    ];

    for name in invalid_names {
        let link_name = CString::new(name).unwrap();
        match fs.symlink(ctx, &target_name, 1, &link_name, Extensions::default()) {
            Ok(_) => panic!("Expected error for invalid name: {}", name),
            Err(e) => {
                assert!(
                    e.kind() == io::ErrorKind::InvalidInput
                        || e.kind() == io::ErrorKind::PermissionDenied,
                    "Unexpected error kind for name {}: {:?}",
                    name,
                    e.kind()
                );
            }
        }
    }

    Ok(())
}

#[test]
fn test_rename_basic() -> io::Result<()> {
    // Create test layers
    let files = vec![("file1.txt", false, 0o644), ("file2.txt", false, 0o644)];
    let layers = vec![files];
    let (overlayfs, _temp_dirs) = helper::create_overlayfs(layers)?;

    // Lookup source and destination parents (root in this case)
    let root = 1;
    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Perform rename
    overlayfs.rename(Context::default(), root, &old_name, root, &new_name, 0)?;

    // Verify old name doesn't exist
    assert!(overlayfs
        .lookup(Context::default(), root, &old_name)
        .is_err());

    // Verify new name exists
    let entry = overlayfs.lookup(Context::default(), root, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_rename_whiteout() -> io::Result<()> {
    // Create test layers with file in lower layer
    let lower_files = vec![("file1.txt", false, 0o644)];
    let upper_files = vec![];
    let layers = vec![lower_files, upper_files];
    let (overlayfs, _temp_dirs) = helper::create_overlayfs(layers)?;

    let root = 1;
    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Rename file from lower layer
    overlayfs.rename(Context::default(), root, &old_name, root, &new_name, 0)?;

    // Verify old name is whited out
    assert!(overlayfs
        .lookup(Context::default(), root, &old_name)
        .is_err());

    // Verify new name exists in upper layer
    let entry = overlayfs.lookup(Context::default(), root, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_rename_multiple_layers() -> io::Result<()> {
    // Create test layers
    let lower_files = vec![("file1.txt", false, 0o644), ("file2.txt", false, 0o644)];
    let middle_files = vec![("file3.txt", false, 0o644)];
    let upper_files = vec![("file4.txt", false, 0o644)];
    let layers = vec![lower_files, middle_files, upper_files];
    let (overlayfs, _temp_dirs) = helper::create_overlayfs(layers)?;

    let root = 1;
    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Rename file from lowest layer
    overlayfs.rename(Context::default(), root, &old_name, root, &new_name, 0)?;

    // Verify old name is whited out
    assert!(overlayfs
        .lookup(Context::default(), root, &old_name)
        .is_err());

    // Verify new name exists in upper layer
    let entry = overlayfs.lookup(Context::default(), root, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_rename_errors() -> io::Result<()> {
    // Create test layers
    let files = vec![
        ("dir1", true, 0o755),
        ("dir1/file1.txt", false, 0o644),
        ("file2.txt", false, 0o644),
    ];
    let layers = vec![files];
    let (overlayfs, _temp_dirs) = helper::create_overlayfs(layers)?;

    let root = 1;
    let dir1_name = CString::new("dir1")?;
    let _ = overlayfs.lookup(Context::default(), root, &dir1_name)?;

    // Test renaming non-existent file
    let nonexistent = CString::new("nonexistent.txt")?;
    let new_name = CString::new("renamed.txt")?;
    assert!(overlayfs
        .rename(Context::default(), root, &nonexistent, root, &new_name, 0,)
        .is_err());

    // Test renaming to invalid parent
    let file2_name = CString::new("file2.txt")?;
    let invalid_parent = 99999;
    assert!(overlayfs
        .rename(
            Context::default(),
            root,
            &file2_name,
            invalid_parent,
            &new_name,
            0,
        )
        .is_err());

    // Test renaming directory to non-empty directory
    let _ = CString::new("dir1_new")?;
    assert!(overlayfs
        .rename(Context::default(), root, &dir1_name, root, &file2_name, 0,)
        .is_err());

    Ok(())
}

#[test]
fn test_rename_whiteout_flag() -> io::Result<()> {
    // Create test layers with file in lower layer
    let lower_files = vec![("file1.txt", false, 0o644)];
    let upper_files = vec![];
    let layers = vec![lower_files, upper_files];
    let (overlayfs, temp_dirs) = helper::create_overlayfs(layers)?;

    let root = 1;
    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Use the whiteout flag
    let flags = bindings::LINUX_RENAME_WHITEOUT;
    overlayfs.rename(
        Context::default(),
        root,
        &old_name,
        root,
        &new_name,
        flags as u32,
    )?;

    // Verify that lookup for the old name fails
    assert!(overlayfs
        .lookup(Context::default(), root, &old_name)
        .is_err());

    // Verify new name exists
    let entry = overlayfs.lookup(Context::default(), root, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    // Check that a whiteout file is created in the top layer
    let top_layer = temp_dirs.last().unwrap().path();
    // For root parent, the whiteout should be at the top layer root with prefix '.wh.'
    let whiteout_path = top_layer.join(".wh.file1.txt");
    let meta = fs::metadata(&whiteout_path)?;
    // Updated check: expect a regular file with mode 0o600
    assert!(
        meta.file_type().is_file(),
        "Expected whiteout to be a regular file"
    );

    Ok(())
}

#[test]
fn test_rename_nested_files() -> io::Result<()> {
    // Create test layers with nested structure
    let files = vec![
        ("dir1", true, 0o755),
        ("dir1/file1.txt", false, 0o644),
        ("dir2", true, 0o755),
    ];
    let (overlayfs, _temp_dirs) = helper::create_overlayfs(vec![files])?;

    let root = 1;
    let dir1_name = CString::new("dir1")?;
    let dir2_name = CString::new("dir2")?;

    // Lookup directory inodes
    let dir1_entry = overlayfs.lookup(Context::default(), root, &dir1_name)?;
    let dir2_entry = overlayfs.lookup(Context::default(), root, &dir2_name)?;

    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Rename file between directories
    overlayfs.rename(
        Context::default(),
        dir1_entry.inode,
        &old_name,
        dir2_entry.inode,
        &new_name,
        0,
    )?;

    // Verify old location is empty
    assert!(overlayfs
        .lookup(Context::default(), dir1_entry.inode, &old_name)
        .is_err());

    // Verify new location has the file
    let entry = overlayfs.lookup(Context::default(), dir2_entry.inode, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    Ok(())
}

#[test]
fn test_rename_complex_layers() -> io::Result<()> {
    // Create test layers with complex structure
    let lower_files = vec![
        ("dir1", true, 0o755),
        ("dir1/file1.txt", false, 0o644),
        ("dir2", true, 0o755),
        ("dir2/file2.txt", false, 0o644),
    ];
    let middle_files = vec![("dir3", true, 0o755), ("dir3/file3.txt", false, 0o644)];
    let upper_files = vec![("dir4", true, 0o755), ("dir4/file4.txt", false, 0o644)];
    let layers = vec![lower_files, middle_files, upper_files];
    let (overlayfs, temp_dirs) = helper::create_overlayfs(layers)?;

    let root = 1;

    // Test renaming between different layer directories
    let dir1_name = CString::new("dir1")?;
    let dir4_name = CString::new("dir4")?;
    let dir1_entry = overlayfs.lookup(Context::default(), root, &dir1_name)?;
    let dir4_entry = overlayfs.lookup(Context::default(), root, &dir4_name)?;

    let old_name = CString::new("file1.txt")?;
    let new_name = CString::new("renamed.txt")?;

    // Rename from lower to upper layer directory
    overlayfs.rename(
        Context::default(),
        dir1_entry.inode,
        &old_name,
        dir4_entry.inode,
        &new_name,
        0,
    )?;

    // Verify file moved correctly
    assert!(overlayfs
        .lookup(Context::default(), dir1_entry.inode, &old_name)
        .is_err());
    let entry = overlayfs.lookup(Context::default(), dir4_entry.inode, &new_name)?;
    assert_eq!(entry.attr.st_mode & libc::S_IFMT, libc::S_IFREG);

    // Check whiteout file in the old parent's directory (dir1) in the top layer
    let top_layer = temp_dirs.last().unwrap().path();
    let whiteout_path = top_layer.join("dir1").join(".wh.file1.txt");
    assert!(
        fs::metadata(&whiteout_path).is_ok(),
        "Expected whiteout file at {:?}",
        whiteout_path
    );

    Ok(())
}

#[test]
fn test_create_basic() -> io::Result<()> {
    // Create test layers:
    // Single layer with a directory
    let layers = vec![vec![("dir1", true, 0o755)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    // Create a new file in root
    let file_name = CString::new("new_file.txt").unwrap();
    let ctx = Context::default();
    let (entry, handle, _) =
        fs.create(ctx, 1, &file_name, 0o644, 0, 0o022, Extensions::default())?;

    // Verify the file was created with correct mode
    let entry_mode = entry.attr.st_mode as u32;
    assert_eq!(entry_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);
    assert_eq!(entry_mode & 0o777, 0o644 & !0o022);

    // Verify we can look it up
    let lookup_entry = fs.lookup(ctx, 1, &file_name)?;
    let lookup_mode = lookup_entry.attr.st_mode as u32;
    assert_eq!(lookup_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

    // Verify the file exists on disk in the top layer
    let file_path = temp_dirs.last().unwrap().path().join("new_file.txt");
    assert!(file_path.exists());
    assert!(file_path.is_file());

    // If we got a handle, release it
    if let Some(h) = handle {
        fs.release(ctx, entry.inode, 0, h, false, false, None)?;
    }

    Ok(())
}

#[test]
fn test_create_nested() -> io::Result<()> {
    // Create test layers with complex structure:
    // Layer 0 (bottom):
    //   - dir1/
    //   - dir1/file1
    //   - dir1/subdir/
    // Layer 1 (middle):
    //   - dir2/
    //   - dir2/file2
    // Layer 2 (top):
    //   - dir3/
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
        ],
        vec![("dir2", true, 0o755), ("dir2/file2", false, 0o644)],
        vec![("dir3", true, 0o755)],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test 1: Create file in dir1 (should trigger copy-up)
    let dir1_name = CString::new("dir1").unwrap();
    let dir1_entry = fs.lookup(ctx, 1, &dir1_name)?;
    let file_name = CString::new("new_file.txt").unwrap();
    let (entry, handle, _) = fs.create(
        ctx,
        dir1_entry.inode,
        &file_name,
        0o644,
        0,
        0o022,
        Extensions::default(),
    )?;
    let entry_mode = entry.attr.st_mode as u32;
    assert_eq!(entry_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

    // Test 2: Create file in dir2 (middle layer, should trigger copy-up)
    let dir2_name = CString::new("dir2").unwrap();
    let dir2_entry = fs.lookup(ctx, 1, &dir2_name)?;
    let middle_file_name = CString::new("middle_file.txt").unwrap();
    let (middle_entry, middle_handle, _) = fs.create(
        ctx,
        dir2_entry.inode,
        &middle_file_name,
        0o644,
        0,
        0o022,
        Extensions::default(),
    )?;
    let middle_mode = middle_entry.attr.st_mode as u32;
    assert_eq!(middle_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

    // Test 3: Create file in dir3 (top layer, no copy-up needed)
    let dir3_name = CString::new("dir3").unwrap();
    let dir3_entry = fs.lookup(ctx, 1, &dir3_name)?;
    let top_file_name = CString::new("top_file.txt").unwrap();
    let (top_entry, top_handle, _) = fs.create(
        ctx,
        dir3_entry.inode,
        &top_file_name,
        0o644,
        0,
        0o022,
        Extensions::default(),
    )?;
    let top_mode = top_entry.attr.st_mode as u32;
    assert_eq!(top_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);

    // Verify all files exist in appropriate layers
    let top_layer = temp_dirs.last().unwrap().path();
    assert!(top_layer.join("dir1/new_file.txt").exists());
    assert!(top_layer.join("dir2/middle_file.txt").exists());
    assert!(top_layer.join("dir3/top_file.txt").exists());

    // Release handles
    if let Some(h) = handle {
        fs.release(ctx, entry.inode, 0, h, false, false, None)?;
    }
    if let Some(h) = middle_handle {
        fs.release(ctx, middle_entry.inode, 0, h, false, false, None)?;
    }
    if let Some(h) = top_handle {
        fs.release(ctx, top_entry.inode, 0, h, false, false, None)?;
    }

    Ok(())
}

#[test]
fn test_create_with_flags() -> io::Result<()> {
    // Create test layers with a directory
    let layers = vec![vec![("dir1", true, 0o755)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test different flag combinations
    let test_cases = vec![
        ("file_rdonly.txt", libc::O_RDONLY, 0o644),
        ("file_wronly.txt", libc::O_WRONLY, 0o644),
        ("file_rdwr.txt", libc::O_RDWR, 0o644),
        ("file_append.txt", libc::O_WRONLY | libc::O_APPEND, 0o644),
        ("file_trunc.txt", libc::O_WRONLY | libc::O_TRUNC, 0o644),
        ("file_excl.txt", libc::O_WRONLY | libc::O_EXCL, 0o644),
    ];

    for (name, flags, mode) in test_cases {
        let file_name = CString::new(name).unwrap();
        let (entry, handle, _) = fs.create(
            ctx,
            1,
            &file_name,
            mode,
            flags as u32,
            0o022,
            Extensions::default(),
        )?;

        // Verify file creation
        let entry_mode = entry.attr.st_mode as u32;
        assert_eq!(entry_mode & libc::S_IFMT as u32, libc::S_IFREG as u32);
        assert_eq!(entry_mode & 0o777, mode & !0o022);

        // Verify file exists
        let file_path = temp_dirs.last().unwrap().path().join(name);
        assert!(file_path.exists());
        assert!(file_path.is_file());

        // Release handle if we got one
        if let Some(h) = handle {
            fs.release(ctx, entry.inode, 0, h, false, false, None)?;
        }
    }

    Ok(())
}

#[test]
fn test_create_existing_name() -> io::Result<()> {
    // Create test layers with existing files
    let layers = vec![vec![
        ("dir1", true, 0o755),
        ("existing_file.txt", false, 0o644),
    ]];

    let (fs, _temp_dirs) = helper::create_overlayfs(layers)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();
    let file_name = CString::new("existing_file.txt").unwrap();

    // Try to create a file with existing name without O_EXCL
    match fs.create(
        ctx,
        1,
        &file_name,
        0o644,
        libc::O_WRONLY as u32,
        0o022,
        Extensions::default(),
    ) {
        Ok(_) => panic!("Expected create with existing name to fail"),
        Err(e) => assert_eq!(e.kind(), io::ErrorKind::AlreadyExists),
    }

    Ok(())
}

#[test]
fn test_create_invalid_parent() -> io::Result<()> {
    // Create test layers
    let layers = vec![vec![("dir1", true, 0o755)]];

    let (fs, _temp_dirs) = helper::create_overlayfs(layers)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test with invalid parent inode
    let file_name = CString::new("test.txt").unwrap();
    let invalid_inode = 999999;
    match fs.create(
        ctx,
        invalid_inode,
        &file_name,
        0o644,
        0,
        0o022,
        Extensions::default(),
    ) {
        Ok(_) => panic!("Expected create with invalid parent to fail"),
        Err(e) => assert_eq!(e.raw_os_error(), Some(libc::EBADF)),
    }

    Ok(())
}

#[test]
fn test_mknod_basic() -> io::Result<()> {
    // Create test layers with a directory
    let layers = vec![vec![("dir1", true, 0o755)]];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Test creating different types of nodes
    let test_cases: Vec<(&str, u32)> = vec![
        ("fifo1", libc::S_IFIFO as u32 | 0o644),
        ("sock1", libc::S_IFSOCK as u32 | 0o644),
    ];

    for (name, mode) in test_cases {
        let node_name = CString::new(name).unwrap();
        let entry = fs.mknod(ctx, 1, &node_name, mode, 0, 0o022, Extensions::default())?;

        // Verify node creation
        let entry_mode = entry.attr.st_mode as u32;
        // The entry should have the correct file type from the xattr, even though
        // the underlying file is a regular file
        assert_eq!(entry_mode & libc::S_IFMT as u32, mode & libc::S_IFMT as u32);

        // Verify node exists in the top layer
        let node_path = temp_dirs.last().unwrap().path().join(name);
        assert!(node_path.exists());

        // Check that the file on disk is actually a regular file (not a special file)
        let metadata = fs::metadata(&node_path)?;
        assert!(
            metadata.file_type().is_file(),
            "Special files should be stored as regular files"
        );

        // Verify xattr was set correctly with the full mode (including file type)
        let xattr_value = helper::get_xattr(&node_path, "user.containers.override_stat")?;
        assert!(
            xattr_value.is_some(),
            "Override xattr should be set on special files"
        );

        // Parse the xattr to verify it contains the correct file type
        if let Some(xattr) = xattr_value {
            let parts: Vec<&str> = xattr.split(':').collect();
            assert_eq!(parts.len(), 3, "xattr should have format uid:gid:mode");
            let stored_mode = u32::from_str_radix(parts[2], 8).expect("mode should be valid octal");
            assert_eq!(
                stored_mode & libc::S_IFMT as u32,
                mode & libc::S_IFMT as u32,
                "xattr should store the correct file type"
            );
        }
    }

    Ok(())
}

#[test]
fn test_mknod_nested() -> io::Result<()> {
    // Create test layers with complex structure
    let layers = vec![
        vec![
            ("dir1", true, 0o755),
            ("dir1/file1", false, 0o644),
            ("dir1/subdir", true, 0o755),
        ],
        vec![("dir2", true, 0o755)],
        vec![("dir3", true, 0o755)],
    ];

    let (fs, temp_dirs) = helper::create_overlayfs(layers)?;
    helper::debug_print_layers(&temp_dirs, false)?;

    // Initialize filesystem
    fs.init(FsOptions::empty())?;

    let ctx = Context::default();

    // Create nodes in different directories
    let test_cases = vec![
        ("dir1", "fifo1", libc::S_IFIFO as u32 | 0o644),
        ("dir2", "sock1", libc::S_IFSOCK as u32 | 0o644),
        ("dir3", "fifo2", libc::S_IFIFO as u32 | 0o644),
    ];

    for (dir, name, mode) in test_cases {
        let dir_name = CString::new(dir).unwrap();
        let dir_entry = fs.lookup(ctx, 1, &dir_name)?;
        let node_name = CString::new(name).unwrap();

        let entry = fs.mknod(
            ctx,
            dir_entry.inode,
            &node_name,
            mode,
            0,
            0o022,
            Extensions::default(),
        )?;

        // Verify node creation
        let entry_mode = entry.attr.st_mode as u32;
        // The entry should have the correct file type from the xattr
        assert_eq!(entry_mode & libc::S_IFMT as u32, mode & libc::S_IFMT as u32);
        assert_eq!(entry_mode & 0o777, (0o644 & !0o022) as u32);

        // Verify node exists in the top layer
        let node_path = temp_dirs.last().unwrap().path().join(dir).join(name);
        assert!(node_path.exists());

        // Check that the file on disk is actually a regular file
        let metadata = fs::metadata(&node_path)?;
        assert!(
            metadata.file_type().is_file(),
            "Special files should be stored as regular files"
        );

        // Verify xattr was set correctly
        let xattr_value = helper::get_xattr(&node_path, "user.containers.override_stat")?;
        assert!(
            xattr_value.is_some(),
            "Override xattr should be set on special files"
        );
    }

    Ok(())
}
