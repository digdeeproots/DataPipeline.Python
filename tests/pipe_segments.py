import asyncio
from typing import Any, Generic, TypeVar

import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import RestructuringSegment, SourceSegment, TransformSegment, SinkSegment, PipeHeadSegment


T = TypeVar('T')


class DataTransferObject:
    some_value: Any
    dumb_object: Any

    def __init__(self, some_value=None):
        self.some_value = some_value
        self.dumb_object = None


class SecondDTO:
    message: str

    def __init__(self, message: str = ""):
        self.message = message


def format_dumb_object(data: DataTransferObject) -> None:
    data.dumb_object = f"{{ saw: {data.some_value} }}"


async def first(data: DataTransferObject) -> dict:
    pass


def parse_first(data: DataTransferObject, new_data: dict) -> None:
    pass


def second(data: DataTransferObject) -> None:
    pass


def extract_for_third(data: SecondDTO) -> dict:
    pass


async def third(data: dict) -> None:
    pass


def convert(data: DataTransferObject) -> SecondDTO:
    return SecondDTO(f"made by convert function from {data.some_value}")


class Capture(Generic[T]):
    result: T

    def __init__(self):
        self.result = None
        self.__name__ = 'Capture'

    def __call__(self, arg: T) -> None:
        self.result = arg


class CaptureAsync(Generic[T]):
    result: T

    def __init__(self):
        self.result = None
        self.__name__ = 'CaptureAsync'

    async def __call__(self, arg: T) -> None:
        await asyncio.sleep(0.005)
        self.result = arg


async def test_restructuring_segment_uses_its_impl():
    def has_right_message(arg: SecondDTO):
        assert_that(arg.message).is_equal_to("made by convert function from 2")

    test_subject = RestructuringSegment(convert, TransformSegment(has_right_message))
    await test_subject.process(DataTransferObject(2))


async def test_pipe_segment_calls_its_transform_impl_to_process_data():
    capture = Capture[DataTransferObject]()
    test_subject = TransformSegment(format_dumb_object, TransformSegment(capture))
    await test_subject.process(DataTransferObject(4))
    assert_that(capture.result.dumb_object).is_equal_to("{ saw: 4 }")


async def test_source_segment_chains_its_two_implementations():
    async def fetch_data(arg: DataTransferObject) -> str:
        await asyncio.sleep(0.005)
        return "fetched value"

    def parse_data(arg: DataTransferObject, new_data: str) -> None:
        arg.some_value = new_data

    capture = Capture[DataTransferObject]()
    test_subject = SourceSegment(fetch_data, parse_data, TransformSegment(capture))
    await test_subject.process(DataTransferObject("original value"))
    assert_that(capture.result.some_value).is_equal_to("fetched value")


async def test_sink_segment_chains_its_two_implementations():
    def extract(arg: DataTransferObject) -> str:
        return "extracted value"

    capture = CaptureAsync[DataTransferObject]()
    test_subject = SinkSegment(extract, capture)
    await test_subject.process(DataTransferObject("original value"))
    assert_that(capture.result).is_equal_to("extracted value")


def test_pipelines_can_be_validated_as_strings():
    test_subject = PipeHeadSegment(
        DataTransferObject,
        SourceSegment(
            first,
            parse_first,
            TransformSegment(
                second,
                RestructuringSegment(
                    convert,
                    SinkSegment(
                        extract_for_third,
                        third)))))
    verify(test_subject.to_verification_string())
