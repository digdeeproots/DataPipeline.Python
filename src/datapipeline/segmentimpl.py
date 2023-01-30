from __future__ import annotations

import inspect
from abc import ABCMeta, abstractmethod
from typing import Generic, TypeVar, Callable, List

from datapipeline import clientapi

U = TypeVar('U')
TIn = TypeVar('TIn')
TOut = TypeVar('TOut')
TRaw = TypeVar('TRaw')


class _PipeSegment(Generic[TIn, TOut], metaclass=ABCMeta):
    _next_segment: _PipeSegment[TOut, U]
    needs: List[str]
    gives: List[str]

    class Iterator:
        next_segment: _PipeSegment

        def __init__(self, next_segment: _PipeSegment):
            self.next_segment = next_segment

        def __iter__(self):
            return self

        def __next__(self):
            if isinstance(self.next_segment, NullTerminator):
                raise StopIteration()
            self.next_segment = self.next_segment._next_segment
            return self.next_segment

    def __init__(self, needs: List[str], gives: List[str], next_segment: _PipeSegment[TOut, U] = None):
        if next_segment is None:
            next_segment = NullTerminator()
        self._next_segment = next_segment
        self.needs = needs
        self.gives = gives

    def __iter__(self):
        return _PipeSegment.Iterator(self)

    @property
    @abstractmethod
    def descriptor(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return f'  |\n{self.descriptor}\n  |     info change: {self.needs} ==> {self.gives}\n{self._next_segment.to_verification_string()}'

    @abstractmethod
    def _process(self, data: TIn) -> TOut:
        pass

    def process(self, data: TIn) -> None:
        result = self._process(data)
        self._next_segment.process(result)


class PipeHeadSegment(_PipeSegment[TIn, TIn], Generic[TIn]):
    _impl: Callable[[], TIn]

    def __init__(self, impl: Callable[[], TIn], next_segment: _PipeSegment[TIn, U] = None):
        super(PipeHeadSegment, self).__init__([], [], next_segment)
        self._impl = impl

    def to_verification_string(self) -> str:
        return f' <!> Start with empty <{self._impl.__name__}>\n{self._next_segment.to_verification_string()}'

    def _process(self, data: TIn) -> TIn:
        return self._impl()

    @property
    def descriptor(self) -> str:
        raise NotImplementedError


class DataProcessingSegment(_PipeSegment[TIn, TIn], Generic[TIn]):
    _impl: clientapi.ProcessingStep[TIn]

    def __init__(self, impl: clientapi.ProcessingStep[TIn], next_segment: _PipeSegment[TIn, U] = None):
        assert isinstance(impl, clientapi.ProcessingStep)
        super(DataProcessingSegment, self).__init__([], [], next_segment)
        self._impl = impl

    @property
    def descriptor(self) -> str:
        return f'{self.symbol()} {self._impl.__name__}'

    @abstractmethod
    def symbol(self) -> str:
        raise NotImplementedError

    def _process(self, data: TIn) -> TIn:
        self._impl(data)
        return data


class SourceSegment(DataProcessingSegment[TIn], Generic[TIn, TRaw]):
    def __init__(self, load: clientapi.Loader[TIn, TRaw], parse: clientapi.ParseImpl[TIn, TRaw],
                 next_segment: _PipeSegment[TIn, U] = None):
        def impl(data: TIn) -> None:
            parse(data, load(data))

        assert isinstance(load, clientapi.Loader)
        assert isinstance(parse, clientapi.ParseImpl)
        impl.__name__ = f'load:{load.__name__}, parse: {parse.__name__}'
        super(SourceSegment, self).__init__(impl, next_segment)

    def symbol(self) -> str:
        return ">-|  "


class TransformSegment(DataProcessingSegment[TIn], Generic[TIn]):
    def symbol(self) -> str:
        return "  +  "


class SinkSegment(DataProcessingSegment[TIn], Generic[TIn, TRaw]):
    def __init__(self, extract: clientapi.Extractor[TIn, TRaw], store: clientapi.StoreImpl[TRaw],
                 next_segment: _PipeSegment[TIn, U] = None):
        def impl(data: TIn) -> None:
            store(extract(data))

        assert isinstance(extract, clientapi.Extractor)
        assert isinstance(store, clientapi.StoreImpl)
        impl.__name__ = f'extract: {extract.__name__}, store: {store.__name__}'
        super(SinkSegment, self).__init__(impl, next_segment)

    def symbol(self) -> str:
        return "  |->"


class RestructuringSegment(_PipeSegment[TIn, TOut], Generic[TIn, TOut]):
    _impl: Callable[[TIn], TOut]

    def __init__(self, impl: Callable[[TIn], TOut], next_segment: _PipeSegment[TOut, U] = None):
        assert isinstance(impl, clientapi.RestructuringStep)
        super(RestructuringSegment, self).__init__([], [], next_segment)
        self._impl = impl

    @property
    def descriptor(self) -> str:
        return f' <->  Changed to <{inspect.get_annotations(self._impl, eval_str=True)["return"].__name__}> using {self._impl.__name__}'

    def _process(self, data: TIn) -> TOut:
        return self._impl(data)


class NullTerminator(_PipeSegment[TIn, TIn], Generic[TIn]):
    def __init__(self):
        pass  # Don't call the superclass.

    def __next__(self):
        raise StopIteration()

    def descriptor(self) -> str:
        raise NotImplementedError

    def to_verification_string(self) -> str:
        return ""

    def process(self, data: TIn) -> None:
        pass

    def _process(self, data: TIn) -> TIn:
        raise NotImplementedError
