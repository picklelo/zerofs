from abc import ABC, abstractmethod
from collections import defaultdict
from stat import S_IFDIR, S_IFREG
from time import time
from typing import Dict, List, Union
from uuid import UUID, uuid4


class FileBase(ABC):
  """Abstract base class for file-like objects."""

  def __init__(self, name: str):
    self.name = name
    self.st_mode = None
    self.st_uid = None
    self.st_gid = None
    self.attrs = {}

  def chmod(self, mode):
    self.st_mode &= 0o770000
    self.st_mode |= mode

  def chown(self, uid, gid):
    self.st_uid = uid
    self.st_gid = gid


class File(FileBase):
  """Represents a file backed by the object store."""

  def __init__(self, file: Dict):
    """Create a file object.

    Args:
      file: A dictionary of file metadata from B2.
    """
    super().__init__(file.get('fileName', ''))
    self.file_id = file.get('fileId', str(uuid4()))
    self.st_size = file.get('contentLength', 0)
    self.st_mtime = file.get('uploadTimestamp', time() * 1e3) * 1e-3
    self.st_ctime = self.st_mtime
    self.st_atime = self.st_mtime
    self.st_mode = S_IFREG | 0o755
    self.st_nlink = 1

  def __repr__(self):
    return '<File {}>'.format(self.name)

  @property
  def is_local_file(self) -> bool:
    """Whether this file is local only, and not on the server."""
    try:
      UUID(self.file_id)
      return True
    except:
      return False

  @property
  def metadata(self) -> Dict:
    return {
        'st_mode': self.st_mode,
        'st_ctime': self.st_ctime,
        'st_mtime': self.st_mtime,
        'st_atime': self.st_atime,
        'st_nlink': self.st_nlink,
        'st_size': self.st_size
    }

  def update(self,
             file_id: str = None,
             file_size: int = None,
             modify_time: str = None,
             access_time: int = None):
    """Update the file metadata.
    Automatically updates the last modified time.

    Args:
      file_id: The new file id.
      file_size: The new file size.
    """
    if file_id:
      self.file_id = file_id
    if file_size:
      self.st_size = file_size
    if modify_time:
      self.st_mtime = modify_time
    if access_time:
      self.st_atime = access_time


class Directory(FileBase):
  """A virtual directory containing subfiles and directories."""

  def __init__(self, name: str, files: List[File], mode=0o755):
    """Initialize with a list of files in this directory.

    Args:
      name: The name of the directory.
      files: A list with file metadata for files in the directory.
      mode: The permissions to set.
    """
    super().__init__(name)

    children = defaultdict(list)
    for file in files:
      filename = file.name.strip('/')
      parts = filename.split('/', 1)
      if len(parts) > 1:
        # This file is in a subdirectory
        parent, path = parts
      else:
        # This file is in this directory
        parent, path = '', filename
      file.name = path
      children[parent].append(file)

    self.files = {file.name: file for file in children['']}
    del children['']
    self.files.update({k: Directory(k, v) for k, v in children.items()})

    self.st_mode = S_IFDIR | mode
    self.st_atime = time()

  @property
  def st_mtime(self) -> float:
    """The last modified time of the directory."""
    if len(self.files) == 0:
      return self.st_atime
    return max([f.st_mtime for f in self.files.values()])

  @property
  def st_nlink(self) -> int:
    """Number of hard links pointing to the directory."""
    return 2 + len([f for f in self.files if type(f) == Directory])

  @property
  def metadata(self) -> Dict:
    return {
        'st_mode': self.st_mode,
        'st_ctime': self.st_mtime,
        'st_mtime': self.st_mtime,
        'st_atime': self.st_mtime,
        'st_nlink': self.st_nlink
    }

  @staticmethod
  def _to_path_list(path: Union[str, List[str]]) -> List[str]:
    """Combine a path to a path list.

    Args:
      path: The path to convert (can be a string or a list)

    Returns:
      A list that can be used to find the node in the tree.
    """
    if type(path) == str:
      path = path.strip('/').split('/')
    return path

  def file_nesting(self, path: Union[str, List[str]]) -> List['Directory']:
    """Get the nesting directories of a file or directory.

    Args:
      path: The path to get the nesting in.

    Returns:
      A list of directories indicating the nested structure of the path.
    """
    path = self._to_path_list(path)
    if path[0] == '':
      return [self]
    file = self.files[path[0]]
    if len(path) == 1:
      # The file is in the root directory
      return [self, file]
    return [self] + file.file_nesting(path[1:])

  def file_at_path(self, path: Union[str, List[str]]) -> File:
    """Get the file given a path relative to this directory.

    Args:
      path: The path of the file to query.

    Returns:
      The file object at the path if it exists.
    """
    return self.file_nesting(path)[-1]

  def file_exists(self, path: str) -> bool:
    """
    Args:
      path: The path to the file to check.

    Returns:
      Whether the file exists.
    """
    try:
      self.file_at_path(path)
      return True
    except KeyError:
      return False

  def _find_node(self, path: Union[str, List[str]]) -> FileBase:
    """Find the node in the directory tree.

    Args:
      path: The path to the node to find.

    Returns:
      The found node.
    """
    path = self._to_path_list(path)
    if len(path) == 0:
      return self

    if (path[0] not in self.files or type(self.files[path[0]]) != Directory):
      raise KeyError('Cannot find node, directory {} does not exist'.format(
          path[0]))

    return self.files[path[0]]._find_node(path[1:])

  def mkdir(self, path: Union[str, List[str]], mode: int):
    """Create a subdirectory.

    Args:
      path: The path to the directory to create.
      mode: The directory permissions.
    """
    path = self._to_path_list(path)
    node = self._find_node(path[:-1])
    if path[-1] in node.files:
      raise KeyError('Directory {} already exists'.format(path))
    node.files[path[-1]] = Directory(path[-1], [], mode=mode)

  def rm(self, path: Union[str, List[str]]):
    """Remove a file or directory.

    Args:
      name: The path to the file.
    """
    nesting = self.file_nesting(path)
    if len(nesting) < 2:
      raise ValueError('Cannot rm the root directory')
    parent_dir, file = nesting[-2:]
    del parent_dir.files[file.name]

  def touch(self, path: Union[str, List[str]], mode: int) -> File:
    """Create an empty file.

    Args:
      path: The path to the file to create.
      mode: The file permissions.
    """
    path = self._to_path_list(path)
    node = self._find_node(path[:-1])
    file = File({'fileName': path[-1]})
    node.files[path[-1]] = file
    return file
