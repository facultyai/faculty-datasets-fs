import os

import fsspec
import pytest

FOLDER_CONTENTS = {f"/folder/data{i}.txt" for i in range(5)}
FOLDER_WITH_CONTENTS = {"/folder"} | FOLDER_CONTENTS


PARAMETRIZE_PREFIX = pytest.mark.parametrize(
    "prefix",
    ["", "/", "faculty-datasets://", "faculty-datasets:///"],
)


@pytest.mark.parametrize(
    "path, expected_result",
    [
        ("", {"/", "/folder", "/data.txt", "/upload.txt"}),
        ("data.txt", {"/data.txt"}),
        ("folder", FOLDER_WITH_CONTENTS),
        ("folder/", FOLDER_WITH_CONTENTS),
        ("folder/data0.txt", {"/folder/data0.txt"}),
    ],
)
@PARAMETRIZE_PREFIX
def test_list(prefix, path, expected_result):
    fs = fsspec.filesystem("faculty-datasets")
    assert set(fs.ls(prefix + path, detail=False)) == expected_result


@PARAMETRIZE_PREFIX
def test_glob(prefix):
    fs = fsspec.filesystem("faculty-datasets")
    assert set(fs.glob(prefix + "folder/da*.txt")) == FOLDER_CONTENTS


def test_read():
    with fsspec.open("faculty-datasets://data.txt") as fp:
        assert fp.read() == b"This is some test data"


def test_read_tail():
    with fsspec.open("faculty-datasets://data.txt") as fp:
        fp.seek(-4, os.SEEK_END)
        assert fp.read() == b"data"


def test_write():
    with fsspec.open("faculty-datasets://upload.txt", "wb") as fp:
        fp.write(b"Test written data")
    with fsspec.open("faculty-datasets://upload.txt") as fp:
        assert fp.read() == b"Test written data"
