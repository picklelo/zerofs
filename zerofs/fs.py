import logging
import os

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from typing import Dict, List

from b2py import B2, utils as b2_utils
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class BackblazeFS(LoggingMixIn, Operations):
  """Virtual filesystem bcked by the B2 object store."""

  BUCKET_NAME = 'abcphotos'

  def __init__(self):
    self.b2 = B2()
    buckets = self.b2.list_buckets()
    bucket = [b for b in buckets if b['bucketName'] == self.BUCKET_NAME]
    if not len(bucket):
      raise ValueError(
          'Create a bucket named {} to enable zerofs.'.format(self.BUCKET_NAME)
      )
    self.bucket_id = bucket[0]['bucketId']
    b2_files = self.b2.list_files(self.bucket_id, limit=1000)

    self.files = {}
    for file in b2_files:
      parent = self.files
      path = file['fileName'].split(os.path)
      for i in range(len(path) - 1):
        if path[i] not in parent:
          parent[path[i]] = {}
        parent = parent[path[i]]
      parent[path[-1]] = file

    self.data = defaultdict(bytes)
    self.fd = 0
    now = time()
    self.files['/'] = dict(
        st_mode=(S_IFDIR | 0o755),
        st_ctime=now,
        st_mtime=now,
        st_atime=now,
        st_nlink=2)
  
  def chmod(self, path, mode):
      self.files[path]['st_mode'] &= 0o770000
      self.files[path]['st_mode'] |= mode
      return 0

  def chown(self, path, uid, gid):
      self.files[path]['st_uid'] = uid
      self.files[path]['st_gid'] = gid

  def create(self, path, mode):
      self.files[path] = dict(
          st_mode=(S_IFREG | mode),
          st_nlink=1,
          st_size=0,
          st_ctime=time(),
          st_mtime=time(),
          st_atime=time())

      self.fd += 1
      return self.fd

  def getattr(self, path, fh=None):
      if path not in self.files:
          raise FuseOSError(ENOENT)

      return self.files[path]

  def getxattr(self, path, name, position=0):
      attrs = self.files[path].get('attrs', {})

      try:
          return attrs[name]
      except KeyError:
          return ''       # Should return ENOATTR

  def listxattr(self, path):
      attrs = self.files[path].get('attrs', {})
      return attrs.keys()

  def mkdir(self, path, mode):
      self.files[path] = dict(
          st_mode=(S_IFDIR | mode),
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
      return self.data[path][offset:offset + size]

  def readdir(self, path, fh):
    files = self._list_files(path)
    return ['.', '..'] + [f['fileName'] for f in files]

  def readlink(self, path):
      return self.data[path]

  def removexattr(self, path, name):
      attrs = self.files[path].get('attrs', {})

      try:
          del attrs[name]
      except KeyError:
          pass        # Should return ENOATTR

  def rename(self, old, new):
      self.data[new] = self.data.pop(old)
      self.files[new] = self.files.pop(old)

  def rmdir(self, path):
      # with multiple level support, need to raise ENOTEMPTY if contains any files
      self.files.pop(path)
      self.files['/']['st_nlink'] -= 1

  def setxattr(self, path, name, value, options, position=0):
      # Ignore options
      attrs = self.files[path].setdefault('attrs', {})
      attrs[name] = value

  def statfs(self, path):
      return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

  def symlink(self, target, source):
      self.files[target] = dict(
          st_mode=(S_IFLNK | 0o777),
          st_nlink=1,
          st_size=len(source))

      self.data[target] = source

  def truncate(self, path, length, fh=None):
      # make sure extending the file fills in zero bytes
      self.data[path] = self.data[path][:length].ljust(
          length, '\x00'.encode('ascii'))
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
          self.data[path][:offset].ljust(offset, '\x00'.encode('ascii'))
          + data
          # and only overwrites the bytes that data is replacing
          + self.data[path][offset + len(data):])
      self.files[path]['st_size'] = len(self.data[path])
      return len(data)


if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('mount')
  args = parser.parse_args()

  logging.basicConfig(level=logging.DEBUG)
  fuse = FUSE(BackblazeFS(), args.mount, foreground=True, allow_other=True)
