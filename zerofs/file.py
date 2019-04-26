from collections import defaultdict
from stat import S_IFDIR, S_IFREG
from time import time
from typing import Dict, List, Union


class File:
  """Represents a file backed by the object store."""

  def __init__(self, file: Dict):
    """Create a file object.

    Args:
      file: A dictionary of file metadata from B2.
    """
    self.name = file['fileName']
    self.file_id = file['fileId']
    self.content_size = file['contentLength']
    self.upload_time = file['uploadTimestamp'] * 1e-3

  def __repr__(self):
    return '<File {}>'.format(self.name)

  @property
  def metadata(self) -> Dict:
    return {
        'st_mode': S_IFREG | 0o755,
        'st_ctime': self.upload_time,
        'st_mtime': self.upload_time,
        'st_atime': self.upload_time,
        'st_nlink': 1,
        'st_size': self.content_size
    }


class Directory:
  """A virtual directory containing subfiles and directories."""

  def __init__(self, files: List[File]):
    """Initialize with a list of files in this directory.

    Args:
      files: A list with file metadata for files in the directory.
    """
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

  @property
  def upload_time(self) -> float:
    if len(self.files) == 0:
      return time()
    return max([f.upload_time for f in self.files.values()])

  @property
  def metadata(self) -> Dict:
    mod_time = self.upload_time
    return {
        'st_mode': S_IFDIR | 0o755,
        'st_ctime': mod_time,
        'st_mtime': mod_time,
        'st_atime': mod_time,
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
