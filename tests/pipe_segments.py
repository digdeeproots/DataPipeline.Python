import pytest
from assertpy import assert_that

from datapipeline import DataProcessingSegment


class ValidSegmentImpl:
    name = "Task 1"


def test_pipe_segment_names_are_used_when_verifying_them():
    processor = ValidSegmentImpl()
    test_subject = DataProcessingSegment(processor)
    assert_that(test_subject.to_verification_string()).contains(processor.name)
