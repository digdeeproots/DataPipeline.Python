import asyncio
from typing import Generic, TypeVar

from approvaltests import verify
from assertpy import assert_that, add_extension

from datapipeline import pipeline, start_with, source, transform, is_valid_pipeline, sink
from examplecode import product

add_extension(is_valid_pipeline)

T = TypeVar('T')


class CaptureAsync(Generic[T]):
    result: T

    def __init__(self):
        self.result = None
        self.__name__ = 'CaptureAsync'

    async def __call__(self, arg: T) -> None:
        await asyncio.sleep(0.005)
        self.result = arg


def test_valid_pipeline_passes_test():
    result = product.create_pipeline()
    assert_that(result).is_valid_pipeline()
    verify(result.to_verification_string())


def test_pipeline_runs_its_segments():
    async def fetch(data: product.RawCustomerData) -> str:
        await asyncio.sleep(0.005)
        return "fetched data"

    def parse(data: product.RawCustomerData, new_data: str) -> None:
        data.value = new_data.split()[0]

    def change(data: product.RawCustomerData) -> None:
        data.value = f"changed {data.value}"

    def extract(data: product.RawCustomerData) -> str:
        return data.value

    capture = CaptureAsync[str]()
    test_subject = pipeline(
        start_with(product.RawCustomerData)
        .then(
            source(fetch, parse),
            transform(change),
            sink(extract, capture))
    )
    test_subject.run()
    assert_that(capture.result).is_equal_to("changed fetched")


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
