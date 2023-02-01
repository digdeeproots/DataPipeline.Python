from __future__ import annotations

import asyncio
import datetime
from typing import Callable, TypeVar, Awaitable, Generic, List, Iterable

from assertpy import soft_assertions

from datapipeline import DataProcessingSegment, RestructuringSegment, PipeHeadSegment, clientapi
from datapipeline.clientapi import AfterError
from datapipeline.segmentimpl import _PipeSegment, SourceSegment, TransformSegment, SinkSegment

T = TypeVar("T")
TRaw = TypeVar("TRaw")
TSrc = TypeVar("TSrc")
TNext = TypeVar("TNext")
TDest = TypeVar("TDest")


def needs(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_needs_ = list(info_items)
        return f

    return inner


def gives(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_gives_ = list(info_items)
        return f

    return inner


SegmentBuilder = Callable[[_PipeSegment[TNext, T] | None], DataProcessingSegment[TNext]]
RestructureBuilder = Callable[[_PipeSegment[TNext, T] | None], RestructuringSegment[TSrc, TNext]]
PipeHeadBuilder = Callable[[_PipeSegment[TNext, T] | None], PipeHeadSegment[TNext]]
AnyBuilder = Callable[[_PipeSegment | None], _PipeSegment]


def start_with(data_constructor: Callable[[], T]) -> IncompletePipeline[T, T]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> PipeHeadSegment[T]:
        return PipeHeadSegment(data_constructor, next_segment)

    return IncompletePipeline[T, T]([build])


SomeException = TypeVar('Exc', bound=Exception)


class ErrorHandler(Generic[T, SomeException]):
    exception: type[SomeException]
    handler: clientapi.ErrorResponse[T, SomeException]

    def __init__(self, exception: type[SomeException], handler: clientapi.ErrorResponse[T, SomeException]) -> None:
        self.exception = exception
        self.handler = handler

    def __call__(self, next_segment: _PipeSegment[T, TNext] | None) -> SourceSegment[T, TRaw]:
        return next_segment


def on_error(exception: type[SomeException],
             handler: clientapi.ErrorResponse[T, SomeException]) -> ErrorHandler[T, SomeException]:
    return ErrorHandler(exception, handler)


def source(load: Callable[[T], Awaitable[TRaw]],
           parse: Callable[[T, TRaw], None],
           *error_handlers: ErrorHandler) -> SegmentBuilder[T]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> SourceSegment[T, TRaw]:
        return SourceSegment(load, parse, next_segment)

    return build


def transform(process: Callable[[T], None], *error_handlers: ErrorHandler) -> SegmentBuilder[T]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> TransformSegment[T]:
        return TransformSegment(process, next_segment)

    return build


def sink(extract: Callable[[TSrc], TDest],
         store: Callable[[TDest], Awaitable[None]],
         *error_handlers: ErrorHandler) -> SegmentBuilder[TSrc]:
    def build(next_segment: _PipeSegment[T, TNext] | None) -> SinkSegment[T, TRaw]:
        return SinkSegment(extract, store, next_segment)

    return build


class IncompletePipeline(Generic[TSrc, T]):
    def __init__(self, prior_steps: List[AnyBuilder]):
        self._prior_steps = prior_steps

    def then(self, *steps: SegmentBuilder[T] | ErrorHandler) -> PotentiallyCompletePipeline[TSrc, T]:
        # Handle error handlers. Require them to only be at the end; they apply to the whole chain.
        return PotentiallyCompletePipeline(self._prior_steps + list(steps))


class PotentiallyCompletePipeline(Generic[TSrc, T]):
    def __init__(self, prior_steps: List[AnyBuilder]):
        self._prior_steps = prior_steps

    def restructure_to(self,
                       data_constructor: Callable[[], TDest],
                       restructure: Callable[[T], TDest]) \
            -> IncompletePipeline[TSrc, TDest]:
        def build(next_segment: _PipeSegment[T, TNext] | None) -> RestructuringSegment[T, TDest]:
            return RestructuringSegment(restructure, next_segment)

        return IncompletePipeline(self._prior_steps + [build])

    def build(self) -> Pipeline:
        next_segment = None
        for builder in reversed(self._prior_steps):
            next_segment = builder(next_segment)
        return Pipeline(next_segment)


class Pipeline:
    _first_segment: _PipeSegment

    def __init__(self, first_segment):
        self._first_segment = first_segment

    def to_verification_string(self):
        return self._first_segment.to_verification_string()

    @property
    def _segments(self) -> Iterable[_PipeSegment]:
        return iter(self._first_segment)

    def run(self):
        asyncio.run(self._first_segment.process(None))


def pipeline(builder: PotentiallyCompletePipeline[TSrc, TDest], *error_handlers: ErrorHandler) -> Pipeline:
    return builder.build()


def retry(max_retries: int, delay: datetime.timedelta) -> clientapi.ErrorResponse:
    retries_left = max_retries

    async def do_retry(
            segment: _PipeSegment[T, TNext],
            original_data: T,
            modified_data: T,
            exception: Exception) -> clientapi.AfterError:
        nonlocal retries_left
        retries_left -= 1
        if retries_left > 0:
            await asyncio.sleep(delay.seconds)
            return clientapi.AfterError.Retry

    return do_retry


def skip_this_step(
        segment: _PipeSegment[T, TNext],
        original_data: T,
        modified_data: T,
        exception: Exception) -> AfterError:
    return AfterError.Skip


def is_valid_pipeline(self):
    if not isinstance(self.val, Pipeline):
        raise TypeError('val must be a pipeline.')
    provided = set()
    with soft_assertions():
        kind = self.kind
        self.kind = 'soft'
        for segment in self.val._segments:
            needed = set(segment.needs)
            if not needed <= provided:
                desc = self.description
                self.description = f'Segment {segment.descriptor}'
                self.error(f'Had unmet needs {needed - provided}. The pipeline to that point had provided {provided}.')
                self.description = desc
            provided.update(segment.gives)
        self.kind = kind
    return self
