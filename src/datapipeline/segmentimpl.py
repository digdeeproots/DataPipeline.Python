from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar, Callable

from datapipeline.clientapi import ProcessingStep, RestructuringStep

U = TypeVar('U')
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class _PipeSegment(Generic[TIn, TOut], metaclass=ABCMeta):
    _next_segment: _PipeSegment[TOut, U]

    def __init__(self, next_segment):
        self._next_segment = next_segment

    @property
    @abstractmethod
    def descriptor(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return f'  |\n{self.descriptor}\n{self._next_segment.to_verification_string()}'

    @abstractmethod
    def _process(self, data: TIn) -> TOut:
        pass

    def process(self, data: TIn) -> None:
        result = self._process(data)
        self._next_segment.process(result)


class DataProcessingSegment(_PipeSegment[TIn, TIn], Generic[TIn]):
    _impl: ProcessingStep[TIn]

    def __init__(self, impl: ProcessingStep[TIn], next_segment: _PipeSegment[TIn, U] = None):
        if next_segment is None:
            next_segment = NullTerminator()
        super(DataProcessingSegment, self).__init__(next_segment)
        self._impl = impl

    @property
    def descriptor(self) -> str:
        return f'  +--{self._impl.__name__}'

    def _process(self, data: TIn) -> TIn:
        self._impl(data)
        return data


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    _impl: Callable[[TIn], TOut]

    def __init__(self, impl: Callable[[TIn], TOut], next_segment: _PipeSegment[TOut, U] = None):
        if next_segment is None:
            next_segment = NullTerminator()
        super(RestructuringSegment, self).__init__(next_segment)
        self._impl = impl

    @property
    def descriptor(self) -> str:
        return f' <-> {self._impl.__name__}'

    def _process(self, data: TIn) -> TOut:
        return self._impl(data)


class NullTerminator(_PipeSegment[TIn, TIn], Generic[TIn]):
    def __init__(self):
        pass  # Don't call the superclass.

    def descriptor(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return ""

    def process(self, data: TIn) -> None:
        pass

    def _process(self, data: TIn) -> TIn:
        raise NotImplementedError

    def then(self, next_segment: ProcessingStep[TIn] | RestructuringStep[TIn, U]) -> _PipeSegment[TIn, U]:
        raise NotImplementedError
