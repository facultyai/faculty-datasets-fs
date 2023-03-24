import os

import fsspec
import pytest

FOLDER_CONTENTS = {"/folder"} | {f"/folder/data{i}.txt" for i in range(5)}


@pytest.mark.parametrize(
    "path, expected_result",
    [
        ("/", {"/", "/folder", "/data.txt", "/upload.txt"}),
        ("/data.txt", {"/data.txt"}),
        ("/folder", FOLDER_CONTENTS),
        ("/folder/", FOLDER_CONTENTS),
        ("/folder/data0.txt", {"/folder/data0.txt"}),
    ],
)
def test_list(path, expected_result):
    fs = fsspec.filesystem("faculty-datasets")
    assert set(fs.ls(path, detail=False)) == expected_result


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
