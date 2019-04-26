from abc import ABC, abstractmethod
from collections import defaultdict
from stat import S_IFDIR, S_IFREG
from time import time
from typing import Dict, List, Union


class FileBase(ABC):
  """Abstract base class for file-like objects."""

  def __init__(self):
    self.st_mode = None
    self.st_uid = None
    self.st_gid = None
    self.attrs = {}

  def chmod(self, mode):
    self.st_mode &= 0o770000
    self.st_mode != mode

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
    super().__init__()
    self.name = file['fileName']
    self.file_id = file['fileId']
    self.st_size = file['contentLength']
    self.st_mtime = file['uploadTimestamp'] * 1e-3
    self.st_ctime = self.st_mtime
    self.st_atime = self.st_mtime
    self.st_mode = S_IFREG | 0o755

  def __repr__(self):
    return '<File {}>'.format(self.name)

  @property
  def metadata(self) -> Dict:
    return {
        'st_mode': self.st_mode,
        'st_ctime': self.st_ctime,
        'st_mtime': self.st_mtime,
        'st_atime': self.st_atime,
        'st_nlink': 1,
        'st_size': self.st_size
    }


class Directory(FileBase):
  """A virtual directory containing subfiles and directories."""

  def __init__(self, files: List[File]):
    """Initialize with a list of files in this directory.

    Args:
      files: A list with file metadata for files in the directory.
    """
    super().__init__()

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
    self.files.update({k: Directory(v) for k, v in children.items()})

    self.st_mode = S_IFDIR | 0o755

  @property
  def st_mtime(self) -> float:
    if len(self.files) == 0:
      return time()
    return max([f.st_mtime for f in self.files.values()])

  @property
  def metadata(self) -> Dict:
    return {
        'st_mode': self.st_mode,
        'st_ctime': self.st_mtime,
        'st_mtime': self.st_mtime,
        'st_atime': self.st_mtime,
        'st_nlink': 2
    }

  def file_at_path(self, path: Union[str, List[str]]) -> File:
    """Get the file given a path relative to this directory.

    Args:
      path: The path of the file to query.

    Returns:
      The file object at the path if it exists.
    """
    if type(path) == str:
      path = path.strip('/').split('/')
    if path[0] == '':
      return self
    file = self.files[path[0]]
    if len(path) == 1:
      return file
    if type(file) == Directory:
      return self.files[path[0]].file_at_path(path[1:])
    raise KeyError('No such directory: {}'.format(path[0]))

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
