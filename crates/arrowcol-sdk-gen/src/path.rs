use std::{
    ffi::OsStr,
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{SdkGenError, SdkGenResult};

/// Workspace-relative output path restricted to a gitignored `.temp` or `target` tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafeOutputPath {
    workspace: PathBuf,
    relative: PathBuf,
    absolute: PathBuf,
}

impl SafeOutputPath {
    /// Validates an output path against the current workspace.
    ///
    /// # Errors
    ///
    /// Returns an error for absolute paths, traversal, symlinked existing
    /// components, paths outside `.temp`/`target`, or paths naming the ignored
    /// root itself.
    pub fn new(workspace: impl AsRef<Path>, relative: impl AsRef<Path>) -> SdkGenResult<Self> {
        let workspace =
            fs::canonicalize(workspace.as_ref()).map_err(|error| SdkGenError::io(workspace.as_ref(), error))?;
        let relative = relative.as_ref();
        if relative.as_os_str().is_empty() || relative.is_absolute() {
            return Err(unsafe_path(relative, "path must be non-empty and workspace-relative"));
        }

        let mut clean = PathBuf::new();
        let mut ignored_root_index = None;
        for (index, component) in relative.components().enumerate() {
            let Component::Normal(segment) = component else {
                return Err(unsafe_path(
                    relative,
                    "path traversal and platform prefixes are forbidden",
                ));
            };
            if segment == OsStr::new(".temp") || segment == OsStr::new("target") {
                ignored_root_index.get_or_insert(index);
            }
            validate_component(segment).map_err(|reason| unsafe_path(relative, reason))?;
            clean.push(segment);
        }
        let Some(ignored_root_index) = ignored_root_index else {
            return Err(unsafe_path(
                relative,
                "output must be below a gitignored .temp or target directory",
            ));
        };
        if ignored_root_index + 1 >= clean.components().count() {
            return Err(unsafe_path(relative, "output must name a child below .temp or target"));
        }

        let absolute = workspace.join(&clean);
        let mut existing = workspace.clone();
        for component in clean.components() {
            let Component::Normal(segment) = component else {
                return Err(unsafe_path(relative, "invalid normalized component"));
            };
            existing.push(segment);
            match fs::symlink_metadata(&existing) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(unsafe_path(relative, "existing path components must not be symlinks"));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => return Err(SdkGenError::io(&existing, error)),
            }
        }

        Ok(Self {
            workspace,
            relative: clean,
            absolute,
        })
    }

    /// Returns the canonical workspace root.
    #[must_use]
    pub fn workspace(&self) -> &Path {
        &self.workspace
    }

    /// Returns the normalized workspace-relative path.
    #[must_use]
    pub fn relative(&self) -> &Path {
        &self.relative
    }

    /// Returns the absolute path below the workspace.
    #[must_use]
    pub fn absolute(&self) -> &Path {
        &self.absolute
    }
}

pub(crate) fn validate_relative_generated_path(path: &Path) -> SdkGenResult<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return Err(SdkGenError::InvalidGeneratedPath(path.to_path_buf()));
    }
    for component in path.components() {
        let Component::Normal(segment) = component else {
            return Err(SdkGenError::InvalidGeneratedPath(path.to_path_buf()));
        };
        validate_component(segment).map_err(|_| SdkGenError::InvalidGeneratedPath(path.to_path_buf()))?;
    }
    Ok(())
}

fn validate_component(component: &OsStr) -> Result<(), &'static str> {
    let Some(component) = component.to_str() else {
        return Err("path components must be UTF-8");
    };
    if component.is_empty() || component == "." || component == ".." {
        return Err("empty and traversal components are forbidden");
    }
    if component.bytes().any(|byte| byte.is_ascii_control()) {
        return Err("control characters are forbidden");
    }
    Ok(())
}

fn unsafe_path(path: &Path, reason: impl Into<String>) -> SdkGenError {
    SdkGenError::UnsafeOutputPath {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}
