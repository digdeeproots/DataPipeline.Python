from __future__ import annotations
import asyncio
from typing import TypeVar, Callable, Awaitable, Generic

from datapipeline import DataProcessingSegment


class DataCollectingDTO:
    pass


class AbstractingDTO:
    pass


T = TypeVar("T")
TRaw = TypeVar("TRaw")
TSrc = TypeVar("TSrc")
TDest = TypeVar("TDest")


def needs(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_needs_ = info_items
        return f

    return inner


def gives(*info_items: str) -> Callable[[T], T]:
    def inner(f: T) -> T:
        f._p_gives_ = info_items
        return f

    return inner


@needs()
@gives("foo from filesystem", "foo list")
async def fetch_one(data: DataCollectingDTO) -> str:
    pass


def parse_one(data: DataCollectingDTO, new_data: str) -> None:
    pass


@needs()
@gives("foo from api", "foo list")
async def fetch_two(data: DataCollectingDTO) -> str:
    pass


def parse_two(data: DataCollectingDTO, new_data: str) -> None:
    pass


@needs("foo list")
@gives("foo.core")
def clean_some(data: DataCollectingDTO) -> None:
    pass


@needs("foo list")
@gives("foo.bar")
def clean_more(data: DataCollectingDTO) -> None:
    pass


@needs("foo.core", "foo.bar")
@gives("related baz")
async def fetch_based_on_data_so_far(data: DataCollectingDTO) -> dict:
    pass


def parse_new_source(data: DataCollectingDTO, new_data: dict) -> None:
    pass


@needs("related baz")
@gives("baz.something")
def clean_a(data: DataCollectingDTO) -> None:
    pass


@needs("foo list", "related baz")
@gives("baz.association")
def clean_b(data: DataCollectingDTO) -> None:
    pass


@needs("baz.something", "baz.association")
@gives("quux")
def clean_c(data: DataCollectingDTO) -> None:
    pass


def restructuring_function(data: DataCollectingDTO) -> AbstractingDTO:
    pass


@needs("foo list", "baz.association", "baz.something")
@gives("alpha", "omega")
def understand_something(data: AbstractingDTO) -> None:
    pass


@needs("foo list", "quux", "alpha")
@gives("beta")
def understand_another(data: AbstractingDTO) -> None:
    pass


@needs("omega", "beta")
@gives("totality")
def keep_understanding(data: AbstractingDTO) -> None:
    pass


class DestStructureOne:
    pass


@needs("alpha", "beta")
def extract_and_format_one(data: AbstractingDTO) -> DestStructureOne:
    pass


async def put_one(data: DestStructureOne) -> None:
    pass


class DestStructureTwo:
    pass


@needs("totality")
def extract_and_format_two(data: AbstractingDTO) -> DestStructureTwo:
    pass


async def put_two(data: DestStructureTwo) -> None:
    pass


def source(load: Callable[[T], Awaitable[TRaw]], parse: Callable[[T, TRaw], None]) -> DataProcessingSegment[T]:
    pass


def transform(process: Callable[[T], None]) -> DataProcessingSegment[T]:
    pass


def sink(extract: Callable[[TSrc], TDest], store: Callable[[TDest], Awaitable[None]]) -> DataProcessingSegment[TSrc]:
    pass


class IncompletePipeline(Generic[TSrc, T]):
    def then(self, *steps: DataProcessingSegment[T]) -> PotentiallyCompletePipeline[TSrc, T]:
        return PotentiallyCompletePipeline()


class PotentiallyCompletePipeline(Generic[TSrc, T]):
    def restructure_to(self,
                       data_constructor: Callable[[], TDest],
                       restructure: Callable[[T], TDest]) \
            -> IncompletePipeline[TSrc, TDest]:
        return IncompletePipeline()


def pipeline(builder: PotentiallyCompletePipeline[TSrc, TDest]):
    pass


def start_with(data_constructor: Callable[[], T]) -> IncompletePipeline[T]:
    return IncompletePipeline()


def create_pipeline():
    return pipeline(
        start_with(DataCollectingDTO)
        .then(
            source(fetch_one, parse_one),
            source(fetch_two, parse_two),
            transform(clean_some),
            transform(clean_more),
            source(fetch_based_on_data_so_far, parse_new_source),
            transform(clean_a),
            transform(clean_b),
            transform(clean_c))
        .restructure_to(AbstractingDTO, restructuring_function)
        .then(
            transform(understand_something),
            transform(understand_another),
            transform(keep_understanding),
            sink(extract_and_format_one, put_one),
            sink(extract_and_format_two, put_two),
        )
    )
