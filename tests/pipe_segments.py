import pytest
from assertpy import assert_that
from approvaltests import verify
from datapipeline import DataProcessingSegment


class DataTransferObject:
    def __init__(self):
        self.some_num = None
        self.dumb_obj = None


class ValidSegmentImpl:
    __name__ = "Task 1"

    def __call__(self, data):
        data.dumb_object = f"{{ saw: {data.some_num} }}"


def first(data):
    pass


def second(data):
    pass


def third(data):
    pass


class SegmentNamed:
    def __init__(self, name):
        self.__name__ = name

    def __call__(self, data):
        raise NotImplementedError


class CapturingSink:
    __name__ = "Capture for test"

    def __init__(self):
        self.result = None

    def __call__(self, data):
        self.result = data


def test_pipe_segment_names_are_used_when_verifying_them():
    processor = ValidSegmentImpl()
    test_subject = DataProcessingSegment(processor)
    assert_that(test_subject.to_verification_string()).contains(processor.__name__)


def test_pipe_segment_calls_its_transform_impl_to_process_data():
    processor = ValidSegmentImpl()
    sink = CapturingSink()
    test_subject = DataProcessingSegment(processor)
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
