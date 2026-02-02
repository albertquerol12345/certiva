"""Abstractions for ingestion sources (local folders, IMAP, SFTP)."""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Protocol

from . import utils


@dataclass
class SourceDocument:
    path: Path
    sha256: str


class IngestionSource(Protocol):
    name: str
    tenant: str

    def list_new_documents(self) -> Iterable[SourceDocument]:
        ...

    def mark_processed(self, document: SourceDocument, doc_id: Optional[str]) -> None:
        ...


class LocalFolderSource:
    """Locate new documents inside a local folder."""

    def __init__(
        self,
        name: str,
        tenant: str,
        root: Path,
        pattern: str = "*.pdf",
        recursive: bool = True,
        archive_dir: Optional[Path] = None,
        max_files: Optional[int] = None,
    ) -> None:
        self.name = name
        self.tenant = tenant
        self.root = Path(root)
        self.pattern = pattern
        self.recursive = recursive
        self.archive_dir = Path(archive_dir) if archive_dir else None
        self.max_files = max_files

    def _iter_paths(self) -> Iterable[Path]:
        if not self.root.exists():
            return []
        iterator = self.root.rglob(self.pattern) if self.recursive else self.root.glob(self.pattern)
        return iterator

    def list_new_documents(self) -> List[SourceDocument]:
        documents: List[SourceDocument] = []
        if not self.root.exists():
            return documents
        for path in self._iter_paths():
            if self.max_files and len(documents) >= self.max_files:
                break
            if not path.is_file():
                continue
            if path.suffix.lower() not in utils.SUPPORTED_EXTENSIONS:
                continue
            sha256 = utils.compute_sha256(path)
            if utils.get_doc(sha256):
                continue
            documents.append(SourceDocument(path=path, sha256=sha256))
        return documents

    def mark_processed(self, document: SourceDocument, doc_id: Optional[str]) -> None:
        if not self.archive_dir:
            return
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        target = self.archive_dir / document.path.name
        counter = 1
        while target.exists():
            target = self.archive_dir / f"{document.path.stem}_{counter}{document.path.suffix}"
            counter += 1
        try:
            shutil.move(str(document.path), target)
        except shutil.Error:
            pass


class ImapSource:
    """Future IMAP ingestion source (emails + attachments).

    This class exposes the expected configuration but the actual network
    implementation is intentionally left as a TODO so that it can be filled in
    once real credentials are available.
    """

    def __init__(
        self,
        name: str,
        tenant: str,
        host: str,
        username: str,
        password: str,
        mailbox: str = "INBOX",
        search: str = "UNSEEN",
        download_dir: Optional[Path] = None,
    ) -> None:
        self.name = name
        self.tenant = tenant
        self.host = host
        self.username = username
        self.password = password
        self.mailbox = mailbox
        self.search = search
        self.download_dir = Path(download_dir or utils.BASE_DIR / "IN" / tenant)

    def _list_local_stub(self) -> List[SourceDocument]:
        documents: List[SourceDocument] = []
        if not self.download_dir.exists():
            return documents
        for path in self.download_dir.glob("*.pdf"):
            sha256 = utils.compute_sha256(path)
            documents.append(SourceDocument(path=path, sha256=sha256))
        return documents

    def list_new_documents(self) -> Iterable[SourceDocument]:
        if not (self.host and self.username and self.password):
            return self._list_local_stub()
        raise NotImplementedError("IMAP ingestion con servidor real no está implementado todavía.")

    def mark_processed(self, document: SourceDocument, doc_id: Optional[str]) -> None:
        if document.path.exists():
            document.path.unlink()


class SftpSource:
    """Future SFTP ingestion source.

    Expected to connect to a remote SFTP, download new PDFs and feed them into
    the pipeline. The actual network logic will be implemented when real
    credentials are configured.
    """

    def __init__(
        self,
        name: str,
        tenant: str,
        host: str,
        username: str,
        password: Optional[str] = None,
        key_path: Optional[Path] = None,
        remote_path: str = "/",
        download_dir: Optional[Path] = None,
    ) -> None:
        self.name = name
        self.tenant = tenant
        self.host = host
        self.username = username
        self.password = password
        self.key_path = key_path
        self.remote_path = remote_path
        self.download_dir = Path(download_dir or utils.BASE_DIR / "IN" / tenant)

    def list_new_documents(self) -> Iterable[SourceDocument]:
        if not (self.host and self.username and (self.password or self.key_path)):
            documents: List[SourceDocument] = []
            if self.download_dir.exists():
                for path in self.download_dir.glob("*.pdf"):
                    documents.append(SourceDocument(path=path, sha256=utils.compute_sha256(path)))
            return documents
        raise NotImplementedError("SFTP ingestion con servidor remoto no está implementado todavía.")

    def mark_processed(self, document: SourceDocument, doc_id: Optional[str]) -> None:
        if document.path.exists():
            document.path.unlink()
