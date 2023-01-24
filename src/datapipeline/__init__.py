from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TypeVar, Generic, runtime_checkable, Protocol

T = TypeVar('T')
U = TypeVar('T')
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


@runtime_checkable
class NamedStep(Protocol):
    @property
    @abstractmethod
    def name(self) -> str:
        pass


@runtime_checkable
class ProcessingStep(NamedStep, Protocol[TIn]):
    @abstractmethod
    def transform(self, data: TIn) -> None:
        pass


@runtime_checkable
class RestructuringStep(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def restructure(self, data: TIn) -> TOut:
        pass


class _PipeSegment(Generic[TIn, TOut], metaclass=ABCMeta):
    _next_segment: _PipeSegment[TOut, U]

    def __init__(self):
        self._next_segment = NullTerminator()

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def to_verification_string(self) -> str:
        return f'+--{self.name}'

    @abstractmethod
    def _process(self, data: TIn) -> TOut:
        pass

    def process(self, data: TIn) -> None:
        result = self._process(data)
        self._next_segment.process(result)

    def then(self, next_segment: ProcessingStep[TOut] | RestructuringStep[TOut, U]) -> _PipeSegment[TOut, U]:
        self._next_segment = DataProcessingSegment(next_segment) if isinstance(next_segment, ProcessingStep) else \
            RestructuringSegment(next_segment)
        return self._next_segment, next_segment


class DataProcessingSegment(_PipeSegment[T, T], Generic[T]):
    _impl: ProcessingStep[T]

    def __init__(self, impl: ProcessingStep[T]):
        super(DataProcessingSegment, self).__init__()
        self._impl = impl

    @property
    def name(self) -> str:
        return self._impl.name

    def _process(self, data: TIn) -> TOut:
        self._impl.transform(data)
        return data


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    _impl: RestructuringStep[TIn, TOut]

    def __init__(self, impl: RestructuringStep[TIn, TOut]):
        super(RestructuringSegment, self).__init__()
        self._impl = impl

    @property
    def name(self) -> str:
        return self._impl.name

    def _process(self, data: TIn) -> TOut:
        return self._impl.restructure(data)


class NullTerminator(_PipeSegment[T, T], Generic[T]):
    def __init__(self):
        pass  # Don't call the superclass.

    def name(self) -> str:
        return "End"

    def process(self, data: TIn) -> None:
        pass

    def _process(self, data: TIn) -> TOut:
        raise NotImplementedError

    def then(self, next_segment: ProcessingStep[T] | RestructuringStep[T, U]) -> _PipeSegment[T, U]:
        raise NotImplementedError
