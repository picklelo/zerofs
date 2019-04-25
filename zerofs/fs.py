import logging
import os

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from typing import Dict, List, Union

from b2py import B2, utils as b2_utils
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class File:

  def __init__(self, file: Dict):
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

  def __init__(self, files: List[File]):
    children = defaultdict(list)
    for file in files:
      filename = file.name.strip('/')
      parts = filename.split('/', 1)
      if len(parts) > 1:
        parent, path = parts
      else:
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
    try:
      self.file_at_path(path)
      return True
    except KeyError:
      return False

class ZeroFS(LoggingMixIn, Operations):
  """Virtual filesystem backed by the B2 object store."""

  def __init__(self, bucket_name: str):
    self.b2 = B2()
    buckets = self.b2.list_buckets()
    bucket = [b for b in buckets if b['bucketName'] == bucket_name]
    if not len(bucket):
      raise ValueError(
        'Create a bucket named {} to enable zerofs.'.format(bucket_name)
      )
    self.bucket_id = bucket[0]['bucketId']
    files = [File(f) for f in self.b2.list_files(self.bucket_id, limit=1000)]

    self.root = Directory(files)
    self.files = {}

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
    if not self.root.file_exists(path):
      raise FuseOSError(ENOENT)
    return self.root.file_at_path(path).metadata

  def getxattr(self, path, name, position=0):
    return ''
    # attrs = self.files[path].get('attrs', {})

    # try:
    #     return attrs[name]
    # except KeyError:
    #     return ''       # Should return ENOATTR

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
    file = self.root.file_at_path(path)
    contents = self.b2.download_file(file.file_id)
    return contents[offset: offset + size]

  def readdir(self, path, _):
    dir = self.root.file_at_path(path)
    return ['.', '..'] + [f for f in dir.files]

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
  parser.add_argument('--bucket', type=str, required=True, help='The B2 bucket to mount')
  parser.add_argument('--background', action='store_true', help='Run in the background')
  parser.add_argument('--verbose', action='store_true', help='Log debug info')
  args = parser.parse_args()

  if args.verbose:
    logging.basicConfig(level=logging.DEBUG)

  fuse = FUSE(
    ZeroFS(args.bucket),
    args.mount,
    foreground=not args.background,
    allow_other=True
  )
