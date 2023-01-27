from typing import Any

import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import RestructuringSegment, SourceSegment, TransformSegment, SinkSegment, PipeHeadSegment


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


def first(data: DataTransferObject) -> None:
    pass


def second(data: DataTransferObject) -> None:
    pass


def third(data: SecondDTO) -> None:
    pass


def convert(data: DataTransferObject) -> SecondDTO:
    return SecondDTO(f"made by convert function from {data.some_num}")


def test_restructuring_segment_uses_its_impl():
    def has_right_message(arg: SecondDTO):
        assert_that(arg.message).is_equal_to("made by convert function from 2")

    test_subject = RestructuringSegment(convert, TransformSegment(has_right_message))
    test_subject.process(DataTransferObject(2))


def test_pipe_segment_calls_its_transform_impl_to_process_data():
    result: Any = None

    def capture(arg: DataTransferObject):
        nonlocal result
        result = arg.dumb_object

    test_subject = TransformSegment(format_dumb_object, TransformSegment(capture))
    data = DataTransferObject(4)
    test_subject.process(data)
    assert_that(result).is_equal_to("{ saw: 4 }")


def test_pipelines_can_be_validated_as_strings():
    test_subject = PipeHeadSegment(
        DataTransferObject,
        SourceSegment(
            first,
            TransformSegment(
                second,
                RestructuringSegment(
                    convert,
                    SinkSegment(
                        third)))))
    verify(test_subject.to_verification_string())
