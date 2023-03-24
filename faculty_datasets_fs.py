from __future__ import annotations

import re
from typing import Iterable
from uuid import UUID

import requests
from faculty.clients.base import NotFound
from faculty.clients.object import (
    CompletedUploadPart,
    Object,
    ObjectClient,
    PathNotFound,
)
from faculty.context import get_context
from faculty.session import get_session
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FacultyDatasetsFileSystem(AbstractFileSystem):
    def __init__(
        self,
        *args,
        project_id: UUID | str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        match project_id:
            case UUID():
                self.project_id = project_id
            case str():
                self.project_id = UUID(project_id)
            case None:
                self.project_id = get_context().project_id

        session = get_session()
        url = session.service_url(ObjectClient.SERVICE_NAME)
        self.object_client = ObjectClient(url, session)

    def ls(self, path, detail=True, **kwargs):
        path = _normalize_path(path)
        path_as_dir = path if path.endswith("/") else path + "/"
        subdir_or_file_pattern = re.compile(
            re.escape(path_as_dir) + r"[^\/]+/?$",
        )

        objects = [
            obj
            for obj in self._ls_objects(path)
            if obj.path in {path, path_as_dir}
            or subdir_or_file_pattern.match(obj.path)
        ]

        if detail:
            return [_file_info_for_obj(obj) for obj in objects]
        else:
            return [_normalize_path(obj.path) for obj in objects]

    def _ls_objects(self, prefix) -> Iterable[Object]:
        list_response = self.object_client.list(self.project_id, prefix)
        yield from list_response.objects

        while list_response.next_page_token is not None:
            list_response = self.object_client.list(
                self.project_id, prefix, list_response.next_page_token
            )
            yield from list_response.objects

    def info(self, path, **kwargs) -> dict:
        file: dict | None

        if path.endswith("/"):
            file = self._info_or_none(path)
        else:
            # Try as directory first
            file = self._info_or_none(path + "/")
            if file is None:
                # Try as regular file
                file = self._info_or_none(path)

        if file is None:
            # Not found in either form
            raise FileNotFoundError(path)
        else:
            return file

    def _info_or_none(self, path) -> dict | None:
        try:
            obj = self.object_client.get(self.project_id, path)
        except NotFound:
            return None
        else:
            return _file_info_for_obj(obj)

    def checksum(self, path):
        return self.info(path)["etag"]

    def _rm(self, path):
        try:
            self.object_client.delete(self.project_id, path)
        except PathNotFound:
            raise FileNotFoundError(path)

    def rm(self, path, recursive=False, maxdepth=None):
        if maxdepth is not None:
            super().rm(path, recursive, maxdepth)
        else:
            try:
                self.object_client.delete(
                    self.project_id,
                    path,
                    recursive=recursive,
                )
            except PathNotFound:
                raise FileNotFoundError(path)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        """Return raw bytes-mode file-like from the file-system"""
        return FacultyDatasetsBufferedFile(
            self.object_client,
            self.project_id,
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )


def _file_info_for_obj(obj: Object) -> dict:
    return {
        "name": _normalize_path(obj.path),
        "type": "directory" if obj.path.endswith("/") else "file",
        "size": obj.size,
        "etag": obj.etag,
    }


def _normalize_path(path: str) -> str:
    return "/" + path.strip("/")


class FacultyDatasetsBufferedFile(AbstractBufferedFile):
    def __init__(
        self, object_client: ObjectClient, project_id: UUID, *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.object_client = object_client
        self.project_id = project_id
        self.download_url: str | None = None

    def _initiate_upload(self):
        self.uploader = _S3ChunkedUploader(
            self.object_client,
            self.project_id,
            self.path,
        )
        self.uploader.initiate()

    def _upload_chunk(self, final):
        self.buffer.seek(0)
        chunk = self.buffer.read(self.blocksize)
        while len(chunk) >= self.blocksize:
            self.uploader.upload_chunk(chunk)
            chunk = self.buffer.read(self.blocksize)
        if final:
            if len(chunk) > 0:
                self.uploader.upload_chunk(chunk)
            self.uploader.finalize()

    def _fetch_range(self, start: int, end: int) -> bytes:
        if self.download_url is None:
            self.download_url = self.object_client.presign_download(
                self.project_id, self.path
            )
        print(f"fetching range {start} - {end}")
        response = requests.get(
            self.download_url,
            headers={"Range": f"bytes={start}-{end}"},
        )
        response.raise_for_status()
        return response.content


class _S3ChunkedUploader:
    def __init__(
        self, object_client: ObjectClient, project_id: UUID, path: str
    ) -> None:
        self.object_client = object_client
        self.project_id = project_id
        self.path = path

        self.upload_id: str | None = None
        self.completed_parts: list[CompletedUploadPart] = []

        self.upload_session = requests.Session()
        # See
        #  https://aws.amazon.com/premiumsupport/knowledge-center/http-5xx-errors-s3
        retries = Retry(
            backoff_factor=0.1,
            status=10,
            status_forcelist=[500, 502, 503, 504],
        )
        self.upload_session.mount("https://", HTTPAdapter(max_retries=retries))

    def initiate(self) -> None:
        response = self.object_client.presign_upload(
            self.project_id,
            self.path,
        )
        self.upload_id = response.upload_id

    def upload_chunk(self, data: bytes) -> None:
        if self.upload_id is None:
            raise Exception("Upload not initiated")

        part_number = len(self.completed_parts) + 1

        chunk_url = self.object_client.presign_upload_part(
            self.project_id,
            self.path,
            self.upload_id,
            part_number,
        )

        upload_response = self.upload_session.put(chunk_url, data=data)
        upload_response.raise_for_status()
        self.completed_parts.append(
            CompletedUploadPart(
                part_number=part_number,
                etag=upload_response.headers["ETag"],
            )
        )

    def finalize(self) -> None:
        if self.upload_id is None:
            raise Exception("Upload not initiated")

        self.object_client.complete_multipart_upload(
            self.project_id,
            self.path,
            self.upload_id,
            self.completed_parts,
        )
