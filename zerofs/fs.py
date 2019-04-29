from logging import getLogger
from threading import Lock
from zerofs.task_queue import TaskQueue

logger = getLogger('zerofs')
  def __init__(self, bucket_name: str, cache_dir: str, cache_size: int,
               upload_delay: float, num_workers: int):
      upload_delay: Delay in seconds after writing before uploading to cloud.
      num_workers: Number of background thread workers.
    logger.info('Initializing zerofs from bucket {}'.format(bucket_name))
    self.file_locks = defaultdict(Lock)
    self.upload_delay = upload_delay

    # Load the directory tree
    logger.info('Loading directory tree')
    # Start the task queue
    logger.info('Starting task queue')
    self.task_queue = TaskQueue(num_workers)
    self.task_queue.start()

    logger.info('chmod %s %s', path, mode)
    logger.info('chown %s %s %s', path, uid, gid)
    logger.info('create %s %s', path, mode)
    file = self.root.touch(path, mode)
    self.cache.add(file.file_id, self._to_bytes(''))
    return ''.encode('utf-8')
    logger.info('mkdir %s %s', path, mode)
    logger.info('read %s %s %s', path, offset, size)
    file = self.root.file_at_path(path)
    logger.info('Found file %s', file.file_id)

    if file.st_size == 0:
      logger.info('File size %s', 0)

    with self.file_locks[file.file_id]:
      # Download from the object store if the file is not cached
      if not self.cache.has(file.file_id):
        logger.info('File not in cache, downloading from store')
        contents = self._to_bytes(self.b2.download_file(file.file_id))
        logger.info('File downloaded %s', len(contents))
        self.cache.add(file.file_id, contents)

      content = self.cache.get(file.file_id)
      logger.info('File size %s', len(content))
      return content[offset:offset + size if size else None]
    logger.info('readdir %s', path)
    logger.info('readlink %s', path)
    return self.read(path, None, 0)
    logger.info('rename %s %s', old, new)
        logger.info('Directory not empty')
    logger.info('rmdir %s', path)
    logger.info('unlink %s', path)
  def _upload_file(self, path: str) -> str:
    """Upload a file to the object store.

    Args:
      path: The path of the file to upload.

    """
    logger.info('upload %s', path)
    file = self.root.file_at_path(path)
    content = self.cache.get(file.file_id)

    with self.file_locks[file.file_id]:
      logger.info('Uploading file %s', len(content))
      response = self.b2.upload_file(self.bucket_id, path.strip('/'), content)
      logger.info('Upload complete')

      logger.info('Updating cache')
      self.cache.delete(file.file_id)
      file.update(file_id=response['fileId'], file_size=len(content))
      self.cache.add(file.file_id, content)

    logger.info('write %s %s %s', path, offset, len(data))
    with self.file_locks[file.file_id]:
      # Delete the exisiting version of the file if it exists
      # self._delete_file(path)

      # Write the new bytes
      data = self._to_bytes(data)
      content = (content[:offset].ljust(offset, self._to_bytes('\x00')) + data +
                 content[offset + len(data):])

      # Immediately save locally
      logger.info('Saving to cache %s %s', file.file_id, len(content))
      file.update(file_size=len(content))
      self.cache.add(file.file_id, content)
      # Submit task to upload to object store
      self.task_queue.submit_task(file.file_id, self.upload_delay,
                                  self._upload_file, path)
      return len(data)