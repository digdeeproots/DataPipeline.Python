import pytest

from datapipeline import PipeSegment
import examplecode

def test_pipe_segment_names_are_used_when_verifying_them():
    test_subject = PipeSegment(examplecode.ValidSegmentImpl)
