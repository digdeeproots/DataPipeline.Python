import pytest
from assertpy import assert_that

from examplecode.product import create_pipeline


def test_create_pipeline_fluently():
    pipeline = create_pipeline()
