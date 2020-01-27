from collections import defaultdict
from enum import Enum
from logging import getLogger
from queue import PriorityQueue
from time import sleep, time
from threading import Thread, Lock
from typing import Callable

logger = getLogger('task_queue')


class Signal(Enum):
  """A special signal to send to a worker queue."""
  STOP = 'stop'


class RunState(Enum):
  """Enum to specify the running state of the task queue."""
  STOPPED = 'stopped'
  RUNNING = 'running'


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
    self.run_state = RunState.STOPPED
    self.threads = []

  def run_worker(self, i, num_retries=5):
    """Function each worker will run.

    Args:
      i: The thread index.
      num_retries; How many times to retry the task.
    """
    logger.info('Initialized task worker %s', i)
    while True:
      # Get the next task.
      task = self.queue.get()

      # Check any special signals.
      if task[1] == Signal.STOP:
        break

      # Otherwise it is a real task to run.
      time_to_run, task_args = task
      task_id, task_version, fn, args, kwargs = task_args
      logger.info('Worker received task %s', task_id)
      logger.info('Task queue size %s', self.queue.qsize())

      # If there is a newer version of the task, skip this one
      with self.task_locks[task_id]:
        if self.tasks[task_id] > task_version:
          logger.info('Task cancelled')
          self.queue.task_done()
          continue

      # Sleep until we are ready to run the code
      time_to_sleep = max(1, time_to_run - time())
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
        except Exception as e:
          logger.info('An error occurred: %s', str(e))
          logger.info('Sleeping %s', backoff)
          sleep(backoff)

      # Put the task back in the queue if we still failed
      if not finished:
        logger.info('Task failed, reinserting into queue %s', task_id)
        self.queue.put(task)

      self.queue.task_done()

    logger.info('Worker %s exiting', i)

  def start(self):
    """Start the background worker threads."""
    if self.run_state == RunState.RUNNING:
      raise ValueError('Task queue already started.')

    for i in range(self.num_workers):
      thread = Thread(target=self.run_worker, args=(i,))
      thread.start()
      self.threads.append(thread)
    self.run_state = RunState.RUNNING

  def stop(self, finish_ongoing_tasks: bool = True):
    """Send signals to stop all worker threads.

    Args:
      finish_ongoing_tasks: If true, finishes all current tasks and then stops
          the worker threads, otherwise stops the threads immediately.
    """
    if self.run_state == RunState.STOPPED:
      raise ValueError('Task queue already stopped.')

    # Gather the queue mutex to clear it and send stop signals.
    if not finish_ongoing_tasks:
      with self.queue.mutex:
        self.queue.clear()

    for i in range(self.num_workers):
      self.queue.put((float('inf'), Signal.STOP))

    logger.info('Waiting for workers to stop.')
    for thread in self.threads:
      thread.join()

    logger.info('Task queue stopped.')
    self.run_state = RunState.STOPPED

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
    if self.run_state == RunState.STOPPED:
      raise ValueError('Start the task queue before submitting tasks.')

    logger.info('Received task %s %s', task_id, delay)
    time_to_run = time() + delay
    args = args or ()
    kwargs = kwargs or {}

    with self.task_locks[task_id]:
      self.tasks[task_id] += 1
      task_version = self.tasks[task_id]

    self.queue.put((time_to_run, (task_id, task_version, fn, args, kwargs)))
    logger.info('Task queue size %s', self.queue.qsize())

  def cancel_task(self, task_id: str):
    """Cancel a submitted task.

    Args:
      task_id: The task to cancel.
    """
    logger.info('Cancel task %s', task_id)
    with self.task_locks[task_id]:
      self.tasks[task_id] += 1
