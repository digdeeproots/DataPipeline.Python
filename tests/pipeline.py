import pytest
from approvaltests import verify
from assertpy import assert_that, add_extension, fail

from datapipeline import pipeline, start_with, source, transform, is_valid_pipeline
from examplecode import product

add_extension(is_valid_pipeline)


def test_valid_pipeline_passes_test():
    result = product.create_pipeline()
    assert_that(result).is_valid_pipeline()
    verify(result.to_verification_string())


def test_pipeline_with_missing_requirement_is_invalid():
    result = pipeline(
        start_with(product.RawCustomerData)
        .then(
            source(product.load_customer_csv, product.parse_customers_from_csv),
            transform(product.remove_empty_orders))
    )

    def make_assertion():
        assert_that(result).is_valid_pipeline()

    assert_that(make_assertion).raises(AssertionError).when_called_with().contains(
        "[Segment   +   remove_empty_orders] Had unmet needs {'customer orders'}. The pipeline to that point had provided {'")
