from abc import ABCMeta, abstractmethod
from typing import TypeVar, Generic

T = TypeVar('T')
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


class PipelineStep(Generic[TIn, TOut], metaclass=ABCMeta):
    @property
    @abstractmethod
    def name(self) -> str:
        pass


class _PipeSegment(Generic[TIn, TOut]):
    def __init__(self, impl: PipelineStep[TIn, TOut]):
        self._impl = impl

    def to_verification_string(self) -> str:
        return f'+--{self._impl.name}'


class DataProcessingSegment(_PipeSegment[T, T], Generic[T]):
    pass


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    pass
