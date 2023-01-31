from __future__ import annotations

from typing import Any

from datapipeline.pipeline import needs, gives, source, transform, sink, pipeline, start_with


class RawCustomerData:
    value: Any

    def __init__(self):
        self.value = None


class CustomerGraph:
    pass


@needs()
@gives("customers from filesystem", "customer list")
async def load_customer_csv(data: RawCustomerData) -> str:
    pass


def parse_customers_from_csv(data: RawCustomerData, new_data: str) -> None:
    pass


@needs()
@gives("customers from api", "customer list")
async def load_customer_crm_api(data: RawCustomerData) -> str:
    pass


def parse_customers_from_json(data: RawCustomerData, new_data: str) -> None:
    pass


@needs("customer list")
@gives("customer emails")
def remove_invalid_emails(data: RawCustomerData) -> None:
    pass


@needs("customer list")
@gives("non-test customers")
def remove_test_customers(data: RawCustomerData) -> None:
    pass


@needs("customer emails", "non-test customers")
@gives("customer orders")
async def load_customer_orders(data: RawCustomerData) -> dict:
    pass


def parse_orders_from_json(data: RawCustomerData, new_data: dict) -> None:
    pass


@needs("customer orders")
@gives("valid orders")
def remove_empty_orders(data: RawCustomerData) -> None:
    pass


@needs("customer list", "customer orders")
@gives("order cohorts")
def group_orders_into_customer_cohorts(data: RawCustomerData) -> None:
    pass


@needs("valid orders", "order cohorts")
@gives("relative order timing")
def compute_cohort_relative_date_per_order(data: RawCustomerData) -> None:
    pass


def create_customer_object_graph(data: RawCustomerData) -> CustomerGraph:
    pass


@needs("customer list", "order cohorts", "customer emails")
@gives("alpha", "omega")
def understand_something(data: CustomerGraph) -> None:
    pass


@needs("customer list", "relative order timing", "alpha")
@gives("beta")
def understand_another(data: CustomerGraph) -> None:
    pass


@needs("omega", "beta")
@gives("totality")
def keep_understanding(data: CustomerGraph) -> None:
    pass


class DestStructureOne:
    pass


@needs("alpha", "beta")
def extract_cohort_analysis(data: CustomerGraph) -> DestStructureOne:
    pass


async def email_analysis_to_sales_team(data: DestStructureOne) -> None:
    pass


class DestStructureTwo:
    pass


@needs("totality")
def extract_revenue_projections(data: CustomerGraph) -> DestStructureTwo:
    pass


async def put_projections_into_quickbooks(data: DestStructureTwo) -> None:
    pass


def create_pipeline():
    return pipeline(
        start_with(RawCustomerData)
        .then(
            source(load_customer_csv, parse_customers_from_csv),
            source(load_customer_crm_api, parse_customers_from_json),
            transform(remove_invalid_emails),
            transform(remove_test_customers),
            source(load_customer_orders, parse_orders_from_json),
            transform(remove_empty_orders),
            transform(group_orders_into_customer_cohorts),
            transform(compute_cohort_relative_date_per_order))
        .restructure_to(CustomerGraph, create_customer_object_graph)
        .then(
            transform(understand_something),
            transform(understand_another),
            transform(keep_understanding),
            sink(extract_cohort_analysis, email_analysis_to_sales_team),
            sink(extract_revenue_projections, put_projections_into_quickbooks),
        )
    )
