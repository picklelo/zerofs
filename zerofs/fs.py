from errno import ENOENT, ENOTEMPTY
  @staticmethod
  def _to_bytes(s: Union[str, bytes]):
    if type(s) == bytes:
      return s
    return s.encode('utf-8')

      raise ValueError('Create a bucket named {} to enable zerofs.'.format(
          self.bucket_name))
  def chmod(self, path: str, mode: int):
    """Change the file permissions.

    Args:
      path: The path to the file.
      mode: The new file mode permissions
    """
  def chown(self, path: str, uid: str, gid: str):
    """Change the file owner.

    Args:
      path: The path to the file.
      uid: The user owner id.
      gid: The group owner id.
    """
  def create(self, path: str, mode: int) -> int:
    """Create an empty file.
    Args:
      path: The path to the file to create.
      mode: The permissions on the file.

    Returns:
      The file descriptor.
    """
    self.root.touch(path, mode)
  def getattr(self, path: str, _) -> Dict:
    """
    Args:
      path: The path to the file.

    Returns:
      The file metadata.
    """
  def getxattr(self, path: str, name: str, _) -> str:
    """Read a file attribute.

    Args:
      path: The path to the file.
      name: The name of the attribute to read.
    
    Returns:
      The value of the attribute for the file.
    """
  def listxattr(self, path: str) -> List[str]:
    """
    Args:
      path: The path to the file.

    Returns:
      The file's extra attributes.
    """
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
    print('reading file', file_id)
    # Special case for empty files
    if len(file_id) == 0:
      return self._to_bytes('')
      print('not in cache, downloading')
      contents = self._to_bytes(self.b2.download_file(file_id))
      self.cache.add(file_id, contents)
    print('reading file', contents)
  def readdir(self, path: str, _) -> List[str]:
    """Read the entries in the directory.

    Args:
      path: The path to the directory.
    
    Returns:
      The names of the entries (files and subdirectories).
    """
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
  def setxattr(self, path: str, name: str, value: str, _, __):
    """Set an attribute for the file.

    Args:
      path: Path to the file.
      name: Name of the attribute to set.
      value: Value of the attribute.
    """
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
  def write(self, path: str, data: str, offset: str, _) -> int:
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
    if self.cache.has(file.file_id):
      self.cache.delete(file.file_id)
    if len(content) > 0:
      self.b2.delete_file(file.file_id, path.strip('/'))

    # Write the new bytes
    data = self._to_bytes(data)
    content = (content[:offset].ljust(offset, self._to_bytes('\x00')) + data +
               content[offset + len(data):])

    # Upload to the object store and save to cache
    response = self.b2.upload_file(self.bucket_id, path.strip('/'), content)
    file.update(file_id=response['fileId'], file_size=len(content))
    self.cache.add(file.file_id, content)
  parser.add_argument('--cache-dir',
                      type=str,
                      help='Cache directory to use',
                      default='~/.zerofs')
  parser.add_argument('--cache-size',
                      type=int,
                      help='Disk cache size in MB',
                      default=5000)
  fuse = FUSE(ZeroFS(args.bucket,
                     cache_dir=cache_dir,
                     cache_size=args.cache_size),