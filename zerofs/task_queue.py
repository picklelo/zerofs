from collections import defaultdict
from logging import getLogger
from queue import PriorityQueue
from time import sleep, time
from threading import Thread, Lock
from typing import Callable


logger = getLogger('task_queue')


class TaskQueue:
  """Class to asynchronously handle tasks in the background."""

  def __init__(self, num_workers: int = 1):
    """Initialize the task queue.

    Args;
      num_workers: How many worker threads to launch to process tasks.
    """
    self.num_workers = num_workers
    self.queue = PriorityQueue()
    # Map from task id to latest version number for that task
    self.tasks = defaultdict(int)
    self.task_locks = defaultdict(Lock)

  def run_worker(self, i, num_retries=5):
    """Function each worker will run.

    Args:
      i: The thread index.
      num_retries; How many times to retry the task.
    """
    logger.info('Initialized task worker %s', i)
    while True:
      task = self.queue.get()
      time_to_run, task_id, task_version, fn, args, kwargs = task
      logger.info('Received task %s', task_id)
      logger.info('Task queue size %s', self.queue.qsize())

      # If there is a newer version of the task, skip this one
      with self.task_locks[task_id]:
        if self.tasks[task_id] > task_version:
          logger.info('Task cancelled')
          self.queue.task_done()
          continue

      # Sleep until we are ready to run the code
      time_to_sleep = min(1, time_to_run - time())
      logger.info('Time to sleep %s', time_to_sleep)
      if time_to_sleep > 0:
        sleep(time_to_sleep)

      # Make this check again
      with self.task_locks[task_id]:
        if self.tasks[task_id] > task_version:
          logger.info('Task cancelled')
          self.queue.task_done()
          continue

      # Run the function, retry on failures
      finished = False
      for backoff in [2**i for i in range(num_retries + 1)]:
        try:
          fn(*args, **kwargs)
          finished = True
          break
        except:
          logger.info('An error occurred, sleeping %s', backoff)
          sleep(backoff)

      # Put the task back in the queue if we still failed
      if not finished:
        logger.info('Task failed, reinserting into queue %s', task_id)
        self.queue.put(task)

      self.queue.task_done()

  def start(self):
    """Start the background worker threads."""
    for i in range(self.num_workers):
      thread = Thread(target=self.run_worker, args=(i,))
      thread.start()

  def submit_task(self, task_id: str, delay: float, fn: Callable, *args,
                  **kwargs):
    """Add a task to run.

    Args:
      task_id: An id to specify the task.
      delay: How much time to wait before running the task.
      fn: The function to run.
      args: The args to pass to fn.
      kwargs: The kwargs to pass to fn.
    """
    logger.info('Received task %s %s', task_id, delay)
    time_to_run = time() + delay
    args = args or ()
    kwargs = kwargs or {}

    with self.task_locks[task_id]:
      self.tasks[task_id] += 1
      task_version = self.tasks[task_id]

    self.queue.put((time_to_run, task_id, task_version, fn, args, kwargs))
    logger.info('Task queue size %s', self.queue.qsize())

  def cancel_task(self, task_id: str):
    """Cancel a submitted task.

    Args:
      task_id: The task to cancel.
    """
    logger.info('Cancel task %s', task_id)
    with self.task_locks[task_id]:
      self.tasks[task_id] += 1
