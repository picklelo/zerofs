import os
import logging
import pathlib
from glob import glob

from b2py import utils as b2_utils

logger = logging.getLogger('cache')


# Default values.
DEFAULT_CACHE_SIZE = 5e9  # 5 GB.

class Cache:
  """Cache files to the local disk to save bandwidth."""

  def __init__(self, cache_dir: pathlib.Path, cache_size: int = DEFAULT_CACHE_SIZE, make_dir: bool = False):
    """Initialize a local object cache.

    Args:
      cache_dir: The directory to save cached files to.
      cache_size: The size, in MB, to limit the cache dir to.
      make_dir: If True, create the cache directory if it does not exist.
    """
    # Check if the cache directory exists.
    if make_dir:
      os.makedirs(cache_dir.name, exist_ok=True)
    assert cache_dir.is_dir(), f'No such directory {cache_dir.name}'
    self.cache_dir = cache_dir

    # Set the cache size (in bytes), and coerce it to an int.
    assert cache_size > 0, 'Cache size must be positive.'
    self.cache_size = int(cache_size)

    # Initialize the index that maps the local file to the path in B2.
    self.index: dict[str, str] = {}

    # The touch list used for the LRU cache.
    self.touch_list: list[str] = []

    # If the
    self._populate_index()

  def _populate_index(self):
    """Read the cache dir and set a local index of records."""
    local_files = glob('{}/*'.format(self.cache_dir))
    for file in local_files:
      self._add_to_index(os.path.basename(file), os.path.getsize(file))

  def touch_file(self, file_id):
    """Move the file to the end of the queue by touching it.

    Args:
      file_id: The file to touch.
    """
    if file_id in self.touch_list:
      self.touch_list.remove(file_id)
    self.touch_list.append(file_id)

  def _recover_disk_space(self):
    """Make sure we stay under our disk space quota."""
    while self.used_disk_space > self.cache_size:
      space_to_recover = self.used_disk_space - self.cache_size
      logger.info('Recovering disk space %s', space_to_recover)
      lru_file = self.touch_list.pop(0)
      file_path = self._path_to_file(lru_file)
      logger.info('Deleting %s', file_path)
      os.remove(file_path)
      del self.index[lru_file]

  def _path_to_file(self, file_id: str):
    """
    Args:
      file_id: The B2 file id to query.

    Returns:
      The local path to the cached file.
    """
    return os.path.join(self.cache_dir, file_id)

  @property
  def used_disk_space(self) -> int:
    """
    Returns:
      The used disk space in bytes.
    """
    return sum(self.index.values())

  def _add_to_index(self, file_id: str, content_size: int):
    """
    Args:
      file_id: The file key.
      content_size: The size of the file's contents.
    """
    self.touch_file(file_id)
    self.index[file_id] = content_size
    self._recover_disk_space()

  def contains(self, file_id):
    """
    Args:
      file_id: The file to check.

    Returns:
      Whether the cache contains the file.
    """
    return file_id in self.index

  def add_file(self, file_id: str, contents: bytes):
    """Add a file to the cache.

    Args:
      file_id: The unique key to look the file up by.
      contents: The file contents.
    """
    file_path = self._path_to_file(file_id)
    b2_utils.write_file(file_path, contents)
    self._add_to_index(file_id, len(contents))

  def update(self, file_id: str, data: bytes, offset: int) -> int:
    """Update an existing file in the cache.

    Args:
      file_id: The file to update.
      data: The data to write to the file.
      offset: The start offset to write the data at.

    Returns:
      The number of bytes written.
    """
    assert offset > 0, f'Offset should be positive, got {offset}'
  
    if not self.contains(file_id):
      raise KeyError(f'Cache does not contain a file with id {file_id}')

    self.touch_file(file_id)
    file_path = self._path_to_file(file_id)

    with open(file_path, 'r+b') as f:
      f.seek(offset)
      return f.write(data)

  def get(self, file_id: str, offset: int = 0, size: int = None) -> bytes:
    """
    Args:
      file_id: The file to read.
      offset: The offset to read from.
      size: The number of bytes to read.
  
    Returns:
      The file's contents.
    """
    if not self.has(file_id):
      raise KeyError('No file {}'.format(file_id))

    self._touch_file(file_id)
    file_path = self._path_to_file(file_id)

    with open(file_path, 'rb') as f:
      f.seek(offset)
      return f.read(size)

  def delete(self, file_id: str):
    """Delete the file from the cache.

    Args:
      file_id: The file to delete.
    """
    file_path = self._path_to_file(file_id)
    os.remove(file_path)
    del self.index[file_id]

  def file_size(self, file_id: int):
    """Get the size of the file in bytes.

    Args:
      file_id: The id of the file in the cache.

    Returns:
      The file size.
    """
    file_path = self._path_to_file(file_id)
    return os.path.getsize(file_path)
