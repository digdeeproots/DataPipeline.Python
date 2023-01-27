from typing import Any

import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import RestructuringSegment, SourceSegment, TransformSegment, SinkSegment


class DataTransferObject:
    some_num: Any
    dumb_object: Any

    def __init__(self):
        self.some_num = None
        self.dumb_object = None


class SecondDTO:
    pass


def format_dumb_object(data: DataTransferObject) -> None:
    data.dumb_object = f"{{ saw: {data.some_num} }}"


def first(data: DataTransferObject) -> None:
    pass


def second(data: DataTransferObject) -> None:
    pass


def third(data: SecondDTO) -> None:
    pass


def convert(data: DataTransferObject) -> SecondDTO:
    pass


def test_pipe_segment_names_are_used_when_verifying_them():
    test_subject = TransformSegment(format_dumb_object)
    assert_that(test_subject.to_verification_string()).contains(format_dumb_object.__name__)


def test_pipe_segment_calls_its_transform_impl_to_process_data():
    result: Any = None

    def capture(arg: DataTransferObject):
        nonlocal result
        result = arg.dumb_object

    test_subject = TransformSegment(format_dumb_object, TransformSegment(capture))
    data = DataTransferObject()
    data.some_num = 4
    test_subject.process(data)
    assert_that(result).is_equal_to("{ saw: 4 }")


def test_pipelines_can_be_validated_as_strings():
    test_subject = SourceSegment(
        first,
        TransformSegment(
            second,
            RestructuringSegment(
                convert,
                SinkSegment(
                    third))))
    verify(test_subject.to_verification_string())
