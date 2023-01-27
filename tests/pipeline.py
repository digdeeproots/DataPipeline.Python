import pytest
from approvaltests import verify
from assertpy import assert_that, add_extension, fail

from datapipeline import pipeline, start_with, source, transform, is_valid_pipeline
from examplecode import product

add_extension(is_valid_pipeline)


def test_valid_pipeline_passes_test():
    pipeline = product.create_pipeline()
    assert_that(pipeline).is_valid_pipeline()
    verify(pipeline.to_verification_string())

def test_pipeline_with_missing_requirement_is_invalid():
    pipe = pipeline(
            start_with(product.RawCustomerData)
            .then(
                source(product.load_customer_csv, product.parse_customers_from_csv),
                transform(product.remove_empty_orders))
        )
    try:
        assert_that(pipe).is_valid_pipeline()
        # fail("Pipeline should not have been valid")
    except AssertionError as e:
        assert_that(str(e)).is_equal_to("")
