from __future__ import annotations

from typing import Callable, TypeVar, Awaitable, Generic

from datapipeline import DataProcessingSegment


T = TypeVar("T")
TRaw = TypeVar("TRaw")
TSrc = TypeVar("TSrc")
TDest = TypeVar("TDest")


def needs(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_needs_ = info_items
        return f

    return inner


def gives(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_gives_ = info_items
        return f

    return inner


def source(load: Callable[[T], Awaitable[TRaw]], parse: Callable[[T, TRaw], None]) -> DataProcessingSegment[T]:
    pass


def transform(process: Callable[[T], None]) -> DataProcessingSegment[T]:
    pass


def sink(extract: Callable[[TSrc], TDest], store: Callable[[TDest], Awaitable[None]]) -> DataProcessingSegment[TSrc]:
    pass


class IncompletePipeline(Generic[TSrc, T]):
    def then(self, *steps: DataProcessingSegment[T]) -> PotentiallyCompletePipeline[TSrc, T]:
        return PotentiallyCompletePipeline()


class PotentiallyCompletePipeline(Generic[TSrc, T]):
    def restructure_to(self,
                       data_constructor: Callable[[], TDest],
                       restructure: Callable[[T], TDest]) \
            -> IncompletePipeline[TSrc, TDest]:
        return IncompletePipeline()


class Pipeline:
    pass


def pipeline(builder: PotentiallyCompletePipeline[TSrc, TDest]) -> Pipeline:
    pass


def start_with(data_constructor: Callable[[], T]) -> IncompletePipeline[T]:
    return IncompletePipeline()
