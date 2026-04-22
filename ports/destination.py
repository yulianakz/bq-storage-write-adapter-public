from typing import Protocol, Sequence, TypeVar

TRow = TypeVar("TRow")
TWriteResult = TypeVar("TWriteResult")


class Destination(Protocol[TRow, TWriteResult]):
    def write(self, rows: Sequence[TRow]) -> TWriteResult:
        ...

    def close(self) -> None:
        ...