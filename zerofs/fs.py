from collections import defaultdict
from errno import ENOATTR, ENOENT, ENOTEMPTY, EINVAL
from stat import S_IFDIR, S_IFLNK
from time import time
from typing import Dict, List, Tuple, Union

from b2py import B2, utils as b2_utils
from fuse import FuseOSError, Operations, LoggingMixIn

from zerofs.cache import Cache
from zerofs.file import Directory, File


class ZeroFS(LoggingMixIn, Operations):
  """Virtual filesystem backed by the B2 object store."""

  def __init__(self, bucket_name: str, cache_dir: str, cache_size: int):
    """Initialize the FUSE filesystem.

    Args:
      bucket_name: The name of the remote bucket to mount.
      cache_dir: The directory to cache files to.
      cache_size: The cache size in MB for saving files on local disk.
    """
    self.bucket_name = bucket_name
    self.cache = Cache(cache_dir, cache_size)
    self.b2 = B2()
    self._load_dir_tree()

  @staticmethod
  def _to_bytes(s: Union[str, bytes]):
    if type(s) == bytes:
      return s
    return s.encode('utf-8')

  def _load_dir_tree(self):
    """Load the directory structure into memory."""
    buckets = self.b2.list_buckets()
    bucket = [b for b in buckets if b['bucketName'] == self.bucket_name]
    if not len(bucket):
      raise ValueError('Create a bucket named {} to enable zerofs.'.format(
          self.bucket_name))
    self.bucket_id = bucket[0]['bucketId']
    files = [File(f) for f in self.b2.list_files(self.bucket_id, limit=10000)]
    self.root = Directory('', files)
    self.fd = 0

  def chmod(self, path: str, mode: int):
    """Change the file permissions.

    Args:
      path: The path to the file.
      mode: The new file mode permissions
    """
    file = self.root.file_at_path(path)
    file.chmod(mode)

  def chown(self, path: str, uid: str, gid: str):
    """Change the file owner.

    Args:
      path: The path to the file.
      uid: The user owner id.
      gid: The group owner id.
    """
    file = self.root.file_at_path(path)
    file.chown(uid, gid)

  def create(self, path: str, mode: int) -> int:
    """Create an empty file.

    Args:
      path: The path to the file to create.
      mode: The permissions on the file.

    Returns:
      The file descriptor.
    """
    self.root.touch(path, mode)
    return self.open()

  def open(self, _=None, __=None) -> int:
    """Increment the file descriptor.

    Returns:
      A new file descriptor.
    """
    self.fd += 1
    return self.fd

  def getattr(self, path: str, _) -> Dict:
    """
    Args:
      path: The path to the file.

    Returns:
      The file metadata.
    """
    if not self.root.file_exists(path):
      raise FuseOSError(ENOENT)
    return self.root.file_at_path(path).metadata

  def getxattr(self, path: str, name: str, _) -> str:
    """Read a file attribute.

    Args:
      path: The path to the file.
      name: The name of the attribute to read.
    
    Returns:
      The value of the attribute for the file.
    """
    file = self.root.file_at_path(path)
    if name in file.attrs:
      return file.attrs[name]
    raise FuseOSError(ENOATTR)

  def listxattr(self, path: str) -> List[str]:
    """
    Args:
      path: The path to the file.

    Returns:
      The file's extra attributes.
    """
    file = self.root.file_at_path(path)
    return file.attrs.keys()

  def mkdir(self, path: str, mode: int):
    """Create a new directory.

    Args:
      path: The path to create.
      mode: The directory permissions.
    """
    self.root.mkdir(path, mode)

  def read(self, path: str, size: int, offset: int, _=None) -> str:
    """Read the file's contents.

    Args:
      path: Theh path to the file to read.
      size: The number of bytes to read.
      offset: The offset to read from.

    Returns:
      The queried bytes of the file.
    """
    file_id = self.root.file_at_path(path).file_id
    if len(file_id) == 0:
      # Special case for empty files
      return self._to_bytes('')
    if not self.cache.has(file_id):
      contents = self._to_bytes(self.b2.download_file(file_id))
      self.cache.add(file_id, contents)
    contents = self.cache.get(file_id)
    return contents[offset:offset + size]

  def readdir(self, path: str, _) -> List[str]:
    """Read the entries in the directory.

    Args:
      path: The path to the directory.
    
    Returns:
      The names of the entries (files and subdirectories).
    """
    dir = self.root.file_at_path(path)
    return ['.', '..'] + [f for f in dir.files]

  def readlink(self, path: str) -> str:
    """Read the entire contents of the file.

    Args:
      path: The file to read.

    Returns:
      The file's contents.
    """
    return self.read(path, -1, 0)

  def removexattr(self, path: str, name: str):
    """Remove an attribute from a file.

    Args:
      path: Path to the file.
      name: Name of the attribute to remove.
    """
    file = self.root.file_at_path(path)
    if name in file.attrs:
      del file.attrs[name]

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

  def rmdir(self, path):
    """Remove a directory, if it is not empty.

    Args:
      path: The path to the directory.
    """
    directory = self.root.file_at_path(path)
    if len(directory.files) > 0:
      return ENOTEMPTY
    self.root.rm(path)

  def setxattr(self, path: str, name: str, value: str, _, __):
    """Set an attribute for the file.

    Args:
      path: Path to the file.
      name: Name of the attribute to set.
      value: Value of the attribute.
    """
    file = self.root.file_at_path(path)
    file.attrs[name] = value

  def statfs(self, _):
    """Get file system stats."""
    return dict(f_bsize=4096, f_blocks=4294967296, f_bavail=4294967296)

  def symlink(self, target, source):
    """Symlink from a target to a source.

    Args:
      target: The symlinked file.
      source: The original file.
    """
    # No support for symlinking
    return FuseOSError(EINVAL)

  def truncate(self, path: str, length: int, _):
    """Truncate or pad the file to the specified length.

    Args:
      path: The file to truncate.
      length: The desired lenght.
    """
    file = self.root.file_at_path(path)
    content = self.readlink(path)
    content = content.ljust(length, '\x00'.encode('utf-8'))
    file.st_size = length

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
    now = time()
    mtime, atime = times if times else (now, now)
    file.update(modify_time=mtime, access_time=atime)

  def write(self, path: str, data: str, offset: str, _=None) -> int:
    """Write data to a file.

    Args:
      path: The file to write to.
      data: The bytes to write.
      offset: The offset in the file to begin writing at.
    
    Returns:
      The number of bytes written.
    """
    file = self.root.file_at_path(path)
    content = self.readlink(path)

    # Delete the exisiting version of the file if it exists
    self._delete_file(path)

    # Write the new bytes
    data = self._to_bytes(data)
    content = (content[:offset].ljust(offset, self._to_bytes('\x00')) + data +
               content[offset + len(data):])

    # Upload to the object store and save to cache
    response = self.b2.upload_file(self.bucket_id, path.strip('/'), content)
    file.update(file_id=response['fileId'], file_size=len(content))
    self.cache.add(file.file_id, content)
    return len(data)
