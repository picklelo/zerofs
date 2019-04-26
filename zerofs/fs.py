import logging
import os

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from typing import Dict, List, Union

from b2py import B2, utils as b2_utils
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

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

    self.files = {}
    self.data = defaultdict(bytes)
    self.fd = 0
    now = time()
    self.files['/'] = dict(st_mode=(S_IFDIR | 0o755),
                           st_ctime=now,
                           st_mtime=now,
                           st_atime=now,
                           st_nlink=2)

  def _load_dir_tree(self):
    """Load the directory structure into memory."""
    buckets = self.b2.list_buckets()
    bucket = [b for b in buckets if b['bucketName'] == self.bucket_name]
    if not len(bucket):
      raise ValueError(
          'Create a bucket named {} to enable zerofs.'.format(self.bucket_name))
    self.bucket_id = bucket[0]['bucketId']
    files = [File(f) for f in self.b2.list_files(self.bucket_id, limit=1000)]
    self.root = Directory(files)

  def chmod(self, path, mode):
    file = self.root.file_at_path(path)
    file.chmod(mode)

  def chown(self, path, uid, gid):
    file = self.root.file_at_path(path)
    file.chown(uid, gid)

  def create(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFREG | mode),
                            st_nlink=1,
                            st_size=0,
                            st_ctime=time(),
                            st_mtime=time(),
                            st_atime=time())

    self.fd += 1
    return self.fd

  def getattr(self, path, fh=None):
    if not self.root.file_exists(path):
      raise FuseOSError(ENOENT)
    return self.root.file_at_path(path).metadata

  def getxattr(self, path, name, _):
    file = self.root.file_at_path(path)
    if name in file.attrs:
      return file.attrs[name]
    return ''

  def listxattr(self, path):
    file = self.root.file_at_path(path)
    return file.attrs.keys()

  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode),
                            st_nlink=2,
                            st_size=0,
                            st_ctime=time(),
                            st_mtime=time(),
                            st_atime=time())

    self.files['/']['st_nlink'] += 1

  def open(self, path, flags):
    self.fd += 1
    return self.fd

  def read(self, path, size, offset, fh):
    file_id = self.root.file_at_path(path).file_id
    if not self.cache.has(file_id):
      self.cache.add(file_id, self.b2.download_file(file_id))
    contents = self.cache.get(file_id)
    return contents[offset:offset + size]

  def readdir(self, path, _):
    dir = self.root.file_at_path(path)
    return ['.', '..'] + [f for f in dir.files]

  def readlink(self, path):
    return self.data[path]

  def removexattr(self, path, name):
    file = self.root.file_at_path(path)
    if name in file.attrs:
      del file.attrs[name]

  def rename(self, old, new):
    self.data[new] = self.data.pop(old)
    self.files[new] = self.files.pop(old)

  def rmdir(self, path):
    # with multiple level support, need to raise ENOTEMPTY if contains any files
    self.files.pop(path)
    self.files['/']['st_nlink'] -= 1

  def setxattr(self, path, name, value, _, __):
    file = self.root.file_at_path(path)
    file.attrs[name] = value

  def statfs(self, path):
    return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

  def symlink(self, target, source):
    self.files[target] = dict(st_mode=(S_IFLNK | 0o777),
                              st_nlink=1,
                              st_size=len(source))

    self.data[target] = source

  def truncate(self, path, length, fh=None):
    # make sure extending the file fills in zero bytes
    self.data[path] = self.data[path][:length].ljust(length,
                                                     '\x00'.encode('ascii'))
    self.files[path]['st_size'] = length

  def unlink(self, path):
    self.data.pop(path)
    self.files.pop(path)

  def utimens(self, path, times=None):
    now = time()
    atime, mtime = times if times else (now, now)
    self.files[path]['st_atime'] = atime
    self.files[path]['st_mtime'] = mtime

  def write(self, path, data, offset, fh):
    self.data[path] = (
        # make sure the data gets inserted at the right offset
        self.data[path][:offset].ljust(offset, '\x00'.encode('ascii')) + data
        # and only overwrites the bytes that data is replacing
        + self.data[path][offset + len(data):])
    self.files[path]['st_size'] = len(self.data[path])
    return len(data)


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('mount')
  parser.add_argument('--bucket',
                      type=str,
                      required=True,
                      help='The B2 bucket to mount')
  parser.add_argument('--background',
                      action='store_true',
                      help='Run in the background')
  parser.add_argument('--cache-dir', type=str, help='Cache directory to use', default='~/.zerofs')
  parser.add_argument('--cache-size', type=int, help='Disk cache size in MB', default=5000)
  parser.add_argument('--verbose', action='store_true', help='Log debug info')
  args = parser.parse_args()

  if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

  cache_dir = os.path.expanduser(args.cache_dir)
  fuse = FUSE(ZeroFS(args.bucket, cache_dir=cache_dir, cache_size=args.cache_size),
              args.mount,
              foreground=not args.background,
              allow_other=True)
