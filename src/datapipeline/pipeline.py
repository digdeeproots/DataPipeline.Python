from __future__ import annotations

from typing import Callable, TypeVar, Awaitable, Generic

from datapipeline import DataProcessingSegment, RestructuringSegment, PipeHeadSegment
from datapipeline.segmentimpl import _PipeSegment, SourceSegment, TransformSegment, SinkSegment

T = TypeVar("T")
TRaw = TypeVar("TRaw")
TSrc = TypeVar("TSrc")
TNext = TypeVar("TNext")
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


SegmentBuilder = Callable[[_PipeSegment[TNext, T] | None], DataProcessingSegment[TNext]]
RestructureBuilder = Callable[[_PipeSegment[TNext, T] | None], RestructuringSegment[TSrc, TNext]]
PipeHeadBuilder = Callable[[_PipeSegment[TNext, T] | None], PipeHeadSegment[TNext]]


def source(load: Callable[[T], Awaitable[TRaw]], parse: Callable[[T, TRaw], None]) -> SegmentBuilder[T]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> SourceSegment[T, TRaw]:
        return SourceSegment(load, parse, next_segment)
    return build


def transform(process: Callable[[T], None]) -> SegmentBuilder[T]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> TransformSegment[T]:
        return TransformSegment(process, next_segment)
    return build


def sink(extract: Callable[[TSrc], TDest], store: Callable[[TDest], Awaitable[None]]) -> SegmentBuilder[TSrc]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> SinkSegment[T, TRaw]:
        return SinkSegment(extract, store, next_segment)
    return build


class IncompletePipeline(Generic[TSrc, T]):
    def then(self, *steps: SegmentBuilder[T]) -> PotentiallyCompletePipeline[TSrc, T]:
        return PotentiallyCompletePipeline(steps)


class PotentiallyCompletePipeline(Generic[TSrc, T]):
    def __init__(self, prior_steps: SegmentBuilder[T]):
        self._prior_steps = prior_steps

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
