from __future__ import annotations

from typing import Callable, TypeVar

T = TypeVar("T")
TRaw = TypeVar("TRaw")
TSrc = TypeVar("TSrc")
TDest = TypeVar("TDest")


def needs(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_needs_ = info_items
        return f

    return inner
