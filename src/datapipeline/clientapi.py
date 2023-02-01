from __future__ import annotations

import enum
from abc import abstractmethod
from typing import runtime_checkable, Protocol, TypeVar, Callable

TIn = TypeVar('TIn')
TOut = TypeVar('TOut')
SomeException = TypeVar('Exc', bound=Exception)


@runtime_checkable
class NamedStep(Protocol):
    @property
    @abstractmethod
    def __name__(self) -> str:
        pass


@runtime_checkable
class ProcessingStep(NamedStep, Protocol[TIn]):
    @abstractmethod
    def __call__(self, data: TIn) -> None:
        pass


@runtime_checkable
class ProcessingStepAsync(NamedStep, Protocol[TIn]):
    @abstractmethod
    async def __call__(self, data: TIn) -> None:
        pass


@runtime_checkable
class RestructuringStep(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn) -> TOut:
        pass


@runtime_checkable
class Loader(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    async def __call__(self, data: TIn) -> TOut:
        pass


@runtime_checkable
class ParseImpl(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn, new_data: TOut) -> None:
        pass


@runtime_checkable
class Extractor(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn) -> TOut:
        pass


@runtime_checkable
class StoreImpl(NamedStep, Protocol[TIn]):
    @abstractmethod
    async def __call__(self, data: TIn) -> None:
        pass


AfterError = enum.Enum('AfterError', ['Abort', 'Retry', 'Skip'])


@runtime_checkable
class ErrorResponseSync(Protocol[TIn, SomeException]):
    @abstractmethod
    def __call__(self, segment: '_PipeSegment', original_data: TIn, modified_data: TIn,
                 exception: SomeException) -> AfterError:
        pass


@runtime_checkable
class ErrorResponseAsync(Protocol[TIn, SomeException]):
    @abstractmethod
    async def __call__(self, segment: '_PipeSegment', original_data: TIn, modified_data: TIn,
                       exception: SomeException) -> AfterError:
        pass


ErrorResponse = ErrorResponseSync[TIn, SomeException] | ErrorResponseAsync[TIn, SomeException]
