//! The cluster key that keys the secret files' HMACs (spec 22.3).
//!
//! One key per cluster: the coordinator generates it on first use and keeps it
//! in a file that only its owner can read; a worker gets a copy when it is
//! enrolled (S10) and keeps it the same way. The file holds 64 lower-case hex
//! characters (32 random bytes). A key file that is a symlink, not a regular
//! file, or readable by anyone else is refused, never "fixed".

use super::plan::{hex_decode, hex_encode};
use super::{Result, SecretError};
use local_ci::source::MIN_SECRET_KEY;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::Path;

const KEY_BYTES: usize = 32;

#[derive(Clone, PartialEq, Eq)]
pub struct ClusterKey(Vec<u8>);

/// Never shows the key.
impl std::fmt::Debug for ClusterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterKey(<redacted>)")
    }
}

impl ClusterKey {
    /// A key from bytes. At least local-ci's minimum, so that a capture never
    /// fails on the key's length.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() < MIN_SECRET_KEY {
            return Err(SecretError::Format(format!(
                "cluster key is {} bytes; at least {MIN_SECRET_KEY} are needed",
                bytes.len()
            )));
        }
        Ok(ClusterKey(bytes))
    }

    pub fn generate() -> Result<Self> {
        let mut b = vec![0u8; KEY_BYTES];
        getrandom::fill(&mut b).map_err(|e| {
            SecretError::Format(format!("no random bytes for the cluster key: {e}"))
        })?;
        Ok(ClusterKey(b))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn to_file_text(&self) -> String {
        format!("{}\n", hex_encode(&self.0))
    }

    fn from_file_text(text: &str) -> Result<Self> {
        let bytes = hex_decode(text.trim_end_matches('\n'))
            .ok_or_else(|| SecretError::Format("cluster key file is not lower-case hex".into()))?;
        ClusterKey::from_bytes(bytes)
    }
}

fn refuse(path: &Path) -> SecretError {
    SecretError::Insecure {
        what: "cluster key file",
        path: path.to_path_buf(),
    }
}

/// Read the key at `path`: a regular file (not a link) with no access for
/// group or others.
pub fn load(path: &Path) -> Result<ClusterKey> {
    let meta = fs::symlink_metadata(path)
        .map_err(|e| SecretError::io(format!("cluster key {}", path.display()), e))?;
    if !meta.file_type().is_file() || meta.mode() & 0o077 != 0 {
        return Err(refuse(path));
    }
    let text = fs::read_to_string(path)
        .map_err(|e| SecretError::io(format!("cluster key {}", path.display()), e))?;
    ClusterKey::from_file_text(&text)
}

/// The key at `path`, made first when there is none. Two processes that race
/// both end with the same key: the file is published with a hard link, which
/// fails for the loser, who then reads the winner's.
pub fn load_or_create(path: &Path) -> Result<ClusterKey> {
    match fs::symlink_metadata(path) {
        Ok(_) => return load(path),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            return Err(SecretError::io(
                format!("cluster key {}", path.display()),
                e,
            ))
        }
    }
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .ok_or_else(|| SecretError::Format("cluster key path has no directory".into()))?;
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| SecretError::io(format!("directory {}", dir.display()), e))?;
    let key = ClusterKey::generate()?;
    let tmp = dir.join(format!(".cluster-key.{}.tmp", ulid::Ulid::new()));
    let io = |e| SecretError::io(format!("cluster key {}", path.display()), e);
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(io)?;
    let wrote = f
        .write_all(key.to_file_text().as_bytes())
        .and_then(|()| f.sync_all());
    drop(f);
    let linked = wrote.and_then(|()| fs::hard_link(&tmp, path));
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(key),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => load(path),
        Err(e) => Err(io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_missing_key_is_created_with_mode_0600_and_read_back() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("state/cluster.key");
        let k = load_or_create(&p).unwrap();
        assert_eq!(k.as_bytes().len(), 32);
        assert_eq!(fs::metadata(&p).unwrap().mode() & 0o777, 0o600);
        assert_eq!(
            fs::metadata(p.parent().unwrap()).unwrap().mode() & 0o777,
            0o700
        );
        assert_eq!(load_or_create(&p).unwrap(), k);
        assert_eq!(load(&p).unwrap(), k);
        // no temp file is left behind
        let names: Vec<_> = fs::read_dir(p.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn two_keys_differ_and_debug_hides_the_key() {
        let a = ClusterKey::generate().unwrap();
        let b = ClusterKey::generate().unwrap();
        assert_ne!(a, b);
        let shown = format!("{a:?}");
        assert!(!shown.contains(&hex_encode(a.as_bytes())));
        assert!(shown.contains("redacted"));
    }

    #[test]
    fn racing_creators_all_end_with_one_key() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("cluster.key");
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let p = p.clone();
                std::thread::spawn(move || load_or_create(&p).unwrap())
            })
            .collect();
        let keys: Vec<ClusterKey> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(keys.iter().all(|k| *k == keys[0]));
        assert_eq!(load(&p).unwrap(), keys[0]);
    }

    #[test]
    fn a_key_file_others_can_read_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("cluster.key");
        let k = load_or_create(&p).unwrap();
        for mode in [0o640, 0o604, 0o644, 0o666] {
            fs::set_permissions(&p, fs::Permissions::from_mode(mode)).unwrap();
            assert!(
                matches!(load(&p), Err(SecretError::Insecure { .. })),
                "{mode:o}"
            );
            assert!(matches!(
                load_or_create(&p),
                Err(SecretError::Insecure { .. })
            ));
        }
        fs::set_permissions(&p, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(load(&p).unwrap(), k);
    }

    #[test]
    fn a_symlink_or_a_directory_is_not_a_key_file() {
        let d = tempfile::tempdir().unwrap();
        let real = d.path().join("real.key");
        let k = load_or_create(&real).unwrap();
        let link = d.path().join("link.key");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(load(&link), Err(SecretError::Insecure { .. })));
        assert!(matches!(
            load_or_create(&link),
            Err(SecretError::Insecure { .. })
        ));
        let dir = d.path().join("dir.key");
        fs::create_dir(&dir).unwrap();
        assert!(matches!(load(&dir), Err(SecretError::Insecure { .. })));
        assert_eq!(load(&real).unwrap(), k);
    }

    #[test]
    fn a_damaged_or_short_key_is_an_error_not_a_new_key() {
        let d = tempfile::tempdir().unwrap();
        for (i, text) in ["zz\n", "ab\n", "", "ABCDEF0123456789ABCDEF0123456789\n"]
            .iter()
            .enumerate()
        {
            let p = d.path().join(format!("k{i}"));
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&p)
                .unwrap();
            f.write_all(text.as_bytes()).unwrap();
            assert!(
                matches!(load_or_create(&p), Err(SecretError::Format(_))),
                "{text:?}"
            );
        }
        assert!(ClusterKey::from_bytes(vec![0; 15]).is_err());
        assert!(ClusterKey::from_bytes(vec![0; 16]).is_ok());
    }

    #[test]
    fn missing_file_and_a_path_without_a_directory() {
        let d = tempfile::tempdir().unwrap();
        assert!(matches!(
            load(&d.path().join("none")),
            Err(SecretError::Io { .. })
        ));
        assert!(matches!(
            load_or_create(Path::new("cluster.key")),
            Err(SecretError::Format(_))
        ));
        // the parent is a file
        let f = d.path().join("f");
        fs::write(&f, b"x").unwrap();
        assert!(load_or_create(&f.join("k")).is_err());
    }
}
