import pytest

from zerofs import task_queue


@pytest.mark.parametrize('num_workers', [1, 2, 5, 10, 32])
def test_set_num_workers(num_workers):
  tq = task_queue.TaskQueue(num_workers=num_workers)
  assert tq.num_workers == num_workers

@pytest.mark.parametrize('num_workers', [0])
def test_set_num_workers(num_workers):
  with pytest.raises(AssertionError):
    task_queue.TaskQueue(num_workers=num_workers)