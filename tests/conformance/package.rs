#[path = "harness/package.rs"]
mod package;
#[test]
fn inventory_is_closed_and_hashes_are_immutable() {
    package::validate(&package::root()).unwrap();
}
#[test]
fn rejects_path_traversal_absolute_paths_and_symlink_escape() {
    let root = tempfile::tempdir().unwrap();
    for path in ["../manifest.yaml", "/etc/passwd", "a/../b", "a\\b", ""] {
        assert!(package::safe_path(root.path(), path).is_err());
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", root.path().join("link")).unwrap();
        assert!(package::safe_path(root.path(), "link").is_err());
    }
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let p = e.unwrap().path();
        let target = to.join(p.file_name().unwrap());
        if p.is_dir() {
            copy_tree(&p, &target);
        } else {
            std::fs::copy(p, target).unwrap();
        }
    }
}
#[test]
fn malformed_inventory_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("conformance/aq-cont-1");
    copy_tree(&package::root(), &root);
    copy_tree(
        &package::root().parent().unwrap().parent().unwrap().join("docs/contracts"),
        &dir.path().join("docs/contracts"),
    );
    copy_tree(
        &package::root().parent().unwrap().parent().unwrap().join("tests"),
        &dir.path().join("tests"),
    );
    package::validate(&root).unwrap();
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.yaml")).unwrap()).unwrap();
    for mutation in 0..6 {
        let mut m = original.clone();
        match mutation {
            0 => {
                let f = m["fixtures"][0].clone();
                m["fixtures"].as_array_mut().unwrap().push(f);
            }
            1 => {
                m["fixtures"][0].as_object_mut().unwrap().remove("sha256");
            }
            2 => m["fixtures"][0]["path"] = "../escape.json".into(),
            3 => m["fixtures"][0]["driver"] = "unknown".into(),
            4 => m["fixtures"][0]["sha256"] = "0".repeat(64).into(),
            _ => {
                std::fs::write(root.join("fixtures/unregistered.json"), b"{}").unwrap();
            }
        }
        std::fs::write(root.join("manifest.yaml"), serde_json::to_vec(&m).unwrap()).unwrap();
        assert!(package::validate(&root).is_err(), "mutation {mutation} passed");
    }
}
