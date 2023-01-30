from typing import Any, Generic, TypeVar

import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import RestructuringSegment, SourceSegment, TransformSegment, SinkSegment, PipeHeadSegment


T = TypeVar('T')


class DataTransferObject:
    some_num: Any
    dumb_object: Any

    def __init__(self, some_num=None):
        self.some_num = some_num
        self.dumb_object = None


class SecondDTO:
    message: str

    def __init__(self, message: str = ""):
        self.message = message


def format_dumb_object(data: DataTransferObject) -> None:
    data.dumb_object = f"{{ saw: {data.some_num} }}"


def first(data: DataTransferObject) -> dict:
    pass


def parse_first(data: DataTransferObject, new_data: dict) -> None:
    pass


def second(data: DataTransferObject) -> None:
    pass


def extract_for_third(data: SecondDTO) -> dict:
    pass


def third(data: dict) -> None:
    pass


def convert(data: DataTransferObject) -> SecondDTO:
    return SecondDTO(f"made by convert function from {data.some_num}")


class Capture(Generic[T]):
    result: T

    def __init__(self):
        self.result = None

    def __call__(self, arg: T) -> None:
        self.result = arg


def test_restructuring_segment_uses_its_impl():
    def has_right_message(arg: SecondDTO):
        assert_that(arg.message).is_equal_to("made by convert function from 2")

    test_subject = RestructuringSegment(convert, TransformSegment(has_right_message))
    test_subject.process(DataTransferObject(2))


def test_pipe_segment_calls_its_transform_impl_to_process_data():
    capture = Capture[DataTransferObject]()
    test_subject = TransformSegment(format_dumb_object, TransformSegment(capture))
    data = DataTransferObject(4)
    test_subject.process(data)
    assert_that(capture.result.dumb_object).is_equal_to("{ saw: 4 }")


def test_source_segment_chains_its_two_implementations():
    def fetch_data(arg: DataTransferObject) -> int:
        return 8

    def parse_data(arg: DataTransferObject, new_data: int) -> None:
        arg.some_num = new_data

    capture = Capture[DataTransferObject]()
    test_subject = SourceSegment(fetch_data, parse_data, TransformSegment(capture))
    test_subject.process(DataTransferObject(4))
    assert_that(capture.result.some_num).is_equal_to(8)


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
