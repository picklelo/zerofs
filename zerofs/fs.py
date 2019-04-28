from errno import ENOATTR, ENOENT, ENOTEMPTY, EINVAL
from stat import S_IFDIR, S_IFLNK
from typing import Dict, List, Tuple, Union
from fuse import FuseOSError, Operations, LoggingMixIn
    files = [File(f) for f in self.b2.list_files(self.bucket_id, limit=10000)]
    self.root = Directory('', files)
    self.fd = 0
    return self.open()

  def open(self, _=None, __=None) -> int:
    """Increment the file descriptor.

    Returns:
      A new file descriptor.
    """
    raise FuseOSError(ENOATTR)
      # Special case for empty files
  def rename(self, old: str, new: str):
    """Rename a file by deleting and recreating it.

    Args:
      old: The old path of the file.
      new: The new path of the file.
    """
    file = self.root.file_at_path(old)
    if type(file) == Directory:
      if len(file.files) > 0:
        return ENOTEMPTY
      self.rmdir(old)
      self.mkdir(new, file.st_mode)
    else:
      contents = self.readlink(old)
      self.unlink(old)
      self.create(new, file.st_mode)
      self.write(new, contents, 0)
    """Remove a directory, if it is not empty.

    Args:
      path: The path to the directory.
    """
    directory = self.root.file_at_path(path)
    if len(directory.files) > 0:
      return ENOTEMPTY
    self.root.rm(path)
  def statfs(self, _):
    """Get file system stats."""
    return dict(f_bsize=4096, f_blocks=4294967296, f_bavail=4294967296)
    """Symlink from a target to a source.
    Args:
      target: The symlinked file.
      source: The original file.
    """
    # No support for symlinking
    return FuseOSError(EINVAL)
  def _delete_file(self, path: str):
    """Delete a file from both the local cache and the object store.

    Args:
      path: The path to the file.
    """
    file = self.root.file_at_path(path)
    if self.cache.has(file.file_id):
      self.cache.delete(file.file_id)
    if file.st_size > 0:
      self.b2.delete_file(file.file_id, path.strip('/'))

  def unlink(self, path: str):
    """Delete a file.

    Args:
      path: The path to the file.
    """
    file = self.root.file_at_path(path)
    if type(file) == Directory:
      self.rmdir(path)
    else:
      self._delete_file(path)
      self.root.rm(path)

  def utimens(self, path: str, times: Tuple[int, int] = None):
    """Update the touch time for the file.
    Args:
      path: The file to update.
      times: The modify times to apply.
    """
    file = self.root.file_at_path(path)
    mtime, atime = times if times else (now, now)
    file.update(modify_time=mtime, access_time=atime)
  def write(self, path: str, data: str, offset: str, _=None) -> int:
    self._delete_file(path)