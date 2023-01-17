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
        self._next_segment = lambda x: None
        self._impl = impl

    def to_verification_string(self) -> str:
        return f'+--{self._impl.name}'

    def process(self, data: TIn):
        self._impl.transform(data)
        self._next_segment(data)

    def __setattr__(self, key, value):
        if key == 'next_segment':
            self._next_segment = value
        super(_PipeSegment, self).__setattr__(key, value)


class DataProcessingSegment(_PipeSegment[T, T], Generic[T]):
    pass


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    pass
