from __future__ import annotations

from abc import abstractmethod
from typing import runtime_checkable, Protocol, TypeVar

TIn = TypeVar('TIn')
TOut = TypeVar('TOut')


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
class RestructuringStep(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn) -> TOut:
        pass


@runtime_checkable
class Loader(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn) -> TOut:
        pass


@runtime_checkable
class ParseImpl(NamedStep, Protocol[TIn, TOut]):
    @abstractmethod
    def __call__(self, data: TIn, new_data: TOut) -> None:
        pass
