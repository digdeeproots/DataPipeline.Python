from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar

from datapipeline.clientapi import ProcessingStep, RestructuringStep

U = TypeVar('U')
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class _PipeSegment(Generic[TIn, TOut], metaclass=ABCMeta):
    _next_segment: _PipeSegment[TOut, U]

    def __init__(self):
        self._next_segment = NullTerminator()

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return f'|\n+--{self.name}\n{self._next_segment.to_verification_string()}'

    @abstractmethod
    def _process(self, data: TIn) -> TOut:
        pass

    def process(self, data: TIn) -> None:
        result = self._process(data)
        self._next_segment.process(result)

    def then(self, next_segment: ProcessingStep[TOut] | RestructuringStep[TOut, U]) -> _PipeSegment[TOut, U]:
        self._next_segment = DataProcessingSegment(next_segment) if isinstance(next_segment, ProcessingStep) else \
            RestructuringSegment(next_segment)
        return self._next_segment


class DataProcessingSegment(_PipeSegment[TIn, TIn], Generic[TIn]):
    _impl: ProcessingStep[TIn]

    def __init__(self, impl: ProcessingStep[TIn]):
        super(DataProcessingSegment, self).__init__()
        self._impl = impl

    @property
    def name(self) -> str:
        return self._impl.__name__

    def _process(self, data: TIn) -> TIn:
        self._impl(data)
        return data


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    _impl: RestructuringStep[TIn, TOut]

    def __init__(self, impl: RestructuringStep[TIn, TOut]):
        super(RestructuringSegment, self).__init__()
        self._impl = impl

    @property
    def name(self) -> str:
        return self._impl.__name__

    def _process(self, data: TIn) -> TOut:
        return self._impl.restructure(data)


class NullTerminator(_PipeSegment[TIn, TIn], Generic[TIn]):
    def __init__(self):
        pass  # Don't call the superclass.

    def name(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return""

    def process(self, data: TIn) -> None:
        pass

    def _process(self, data: TIn) -> TIn:
        raise NotImplementedError

    def then(self, next_segment: ProcessingStep[TIn] | RestructuringStep[TIn, U]) -> _PipeSegment[TIn, U]:
        raise NotImplementedError
