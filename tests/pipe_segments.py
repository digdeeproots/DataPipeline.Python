import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import DataProcessingSegment


class DataTransferObject:
    def __init__(self):
        self.some_num = None
        self.dumb_obj = None


def format_dumb_object(data: DataTransferObject) -> None:
    data.dumb_object = f"{{ saw: {data.some_num} }}"


def first(data: DataTransferObject) -> None:
    pass


def second(data: DataTransferObject) -> None:
    pass


def third(data: DataTransferObject) -> None:
    pass


class SegmentNamed:
    def __init__(self, name):
        self.__name__ = name

    def __call__(self, data: DataTransferObject) -> None:
        raise NotImplementedError


class CapturingSink:
    __name__ = "Capture for test"
    result: DataTransferObject

    def __init__(self):
        self.result = None

    def __call__(self, data: DataTransferObject) -> None:
        self.result = data


def test_pipe_segment_names_are_used_when_verifying_them():
    test_subject = DataProcessingSegment(format_dumb_object)
    assert_that(test_subject.to_verification_string()).contains(format_dumb_object.__name__)


def test_pipe_segment_calls_its_transform_impl_to_process_data():
    sink = CapturingSink()
    test_subject = DataProcessingSegment(format_dumb_object)
    test_subject.then(sink)
    data = DataTransferObject()
    data.some_num = 4
    test_subject.process(data)
    assert_that(sink.result.dumb_object).is_equal_to("{ saw: 4 }")


def test_pipelines_can_be_validated_as_strings():
    test_subject = DataProcessingSegment(SegmentNamed("first"))
    test_subject \
        .then(SegmentNamed("second"))\
        .then(SegmentNamed("third"))
    verify(test_subject.to_verification_string())
