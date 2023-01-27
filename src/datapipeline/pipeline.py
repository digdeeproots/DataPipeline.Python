from __future__ import annotations

from typing import Callable, TypeVar, Awaitable, Generic

from datapipeline import DataProcessingSegment, RestructuringSegment

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


SegmentBuilder = Callable[[DataProcessingSegment[T] | None], DataProcessingSegment[T]]
RestructureBuilder = Callable[[DataProcessingSegment[TDest] | None], RestructuringSegment[TSrc, TDest]]


def source(load: Callable[[T], Awaitable[TRaw]], parse: Callable[[T, TRaw], None]) -> SegmentBuilder[T]:
    pass


def transform(process: Callable[[T], None]) -> SegmentBuilder[T]:
    pass


def sink(extract: Callable[[TSrc], TDest], store: Callable[[TDest], Awaitable[None]]) -> SegmentBuilder[TSrc]:
    pass


class IncompletePipeline(Generic[TSrc, T]):
    def then(self, *steps: SegmentBuilder[T]) -> PotentiallyCompletePipeline[TSrc, T]:
        return PotentiallyCompletePipeline()


class PotentiallyCompletePipeline(Generic[TSrc, T]):
    def restructure_to(self,
                       data_constructor: Callable[[], TDest],
                       restructure: Callable[[T], TDest]) \
            -> IncompletePipeline[TSrc, TDest]:
        return IncompletePipeline()


class Pipeline:
    def __init__(self):
        self.segments = []

    def to_verification_string(self):
        return ""


def pipeline(builder: PotentiallyCompletePipeline[TSrc, TDest]) -> Pipeline:
    return Pipeline()


def start_with(data_constructor: Callable[[], T]) -> IncompletePipeline[T]:
    return IncompletePipeline()


def is_valid_pipeline(self):
    if not isinstance(self.val, Pipeline):
        raise TypeError('val must be a pipeline.')
    for segment in self.val.segments:
        if segment != 5:
            self.error(f'{self.val} is NOT 5!')
    return self
