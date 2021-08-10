import pathlib
import tempfile

import pytest

from zerofs.cache import Cache

@pytest.fixture
def cache_dir():
  tempdir = tempfile.TemporaryDirectory()
  yield pathlib.Path(tempdir.name)
  tempdir.cleanup()

@pytest.fixture
def cache(cache_dir: pathlib.Path):
  return Cache(cache_dir=cache_dir)

@pytest.mark.parametrize(
  'cache_dir',
  ['/not/a/real/path', '/another/fake/path']
)
def test_invalid_cache_dir(cache_dir: str):
  cache_dir = pathlib.Path(cache_dir)
  print(cache_dir, cache_dir.is_dir(), cache_dir.exists()) 
  with pytest.raises(AssertionError):
    Cache(cache_dir=pathlib.Path(cache_dir))

def test_valid_cache_dir(cache_dir: pathlib.Path, cache: Cache):
  assert cache.cache_dir == cache_dir
  assert cache.cache_size > 0

@pytest.mark.parametrize(
  'cache_size',
  [-10, -1, 0]
)
def test_invalid_cache_size(cache_dir: pathlib.Path, cache_size: int):
  with pytest.raises(AssertionError):
    Cache(cache_dir=cache_dir, cache_size=cache_size)

@pytest.mark.parametrize(
  'cache_size',
  [1, 100, 5000]
)
def test_valid_cache_size(cache_dir: pathlib.Path, cache_size: int):
  cache = Cache(cache_dir=cache_dir, cache_size=cache_size)
  assert cache.cache_size == cache_size

@pytest.mark.parametrize(
  'file_id,contents',
  [
    ('file1.txt', b'contents1'),
    ('file2.txt', b''),
    ('file1.txt', b'*' * 5000),
  ]
)
def test_add_and_contains_file(cache: Cache, file_id: str, contents: bytes):
  assert not cache.contains(file_id=file_id)
  cache.add_file(file_id=file_id, contents=contents)
  assert cache.contains(file_id=file_id)
  assert cache.index[file_id] == len(contents)

