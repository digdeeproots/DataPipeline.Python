from __future__ import annotations

from abc import abstractmethod
from typing import runtime_checkable, Protocol, TypeVar

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
