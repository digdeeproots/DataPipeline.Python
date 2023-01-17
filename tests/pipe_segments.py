import pytest
from assertpy import assert_that

from datapipeline import DataProcessingSegment


class DataTransferObject:
    def __init__(self):
        self.some_num = None
        self.dumb_obj = None


class ValidSegmentImpl:
    name = "Task 1"

    def transform(self, data):
        data.dumb_object = f"{{ saw: {data.some_num} }}"

def test_pipe_segment_names_are_used_when_verifying_them():
    processor = ValidSegmentImpl()
    test_subject = DataProcessingSegment(processor)
    assert_that(test_subject.to_verification_string()).contains(processor.name)

def test_pipe_segment_calls_its_transform_impl_to_process_data():
    processor = ValidSegmentImpl()
    test_subject = DataProcessingSegment(processor)
    data = DataTransferObject()
    data.some_num = 4
    result = None
    def store_next(input):
        nonlocal result
        result = input
    test_subject.next_segment = store_next
    test_subject.process(data)
    assert_that(result.dumb_object).is_equal_to("{ saw: 4 }")
