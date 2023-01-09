from  assertpy import assert_that
from project.examplecode import start_bank, start_currency_exchange, account_balance, Currency, StandardAccounts, \
    create_account, transfer_new as transfer, account_log, LogEntry


def test_bank_starts_with_right_balance():
    bank = start_bank(10000, Currency.Dollars)
    assert_that(account_balance(bank, StandardAccounts.Cash)).is_equal_to(10000)


def test_simple_transfer():
    bank = start_bank(10000, Currency.Dollars)
    source = create_account(bank)
    destination = create_account(bank)
    source. deposit(200)
    destination.deposit(100)
    transfer(source, destination, 150)
    assert_that(account_balance(bank, source.number)).is_equal_to(50)
    assert_that(account_balance(bank, destination.number)).is_equal_to(250)


def test_nsf_transfer_no_fee():
    bank = start_bank(10000, Currency.Dollars)
    source = create_account(bank)
    destination = create_account(bank)
    source. deposit(50)
    destination.deposit(100)
    transfer(source, destination, 150)
    assert_that(account_balance(bank, source.number)).is_equal_to(50)
    assert_that(account_balance(bank, destination.number)).is_equal_to(100)


def test_nsf_transfer_with_fee():
    bank = start_bank(10000, Currency.Dollars)
    nsf_fee = 15
    source = create_account(bank)
    destination = create_account(bank)
    source. deposit(50)
    destination.deposit(100)
    transfer(source, destination, 150, nsf_fee)
    assert_that(account_balance(bank, source.number)).is_equal_to(50 - nsf_fee)
    source_log = account_log(bank, source.number)
    assert_that(source_log).extracting('reason', 'amount').contains(("Insufficient funds for transfer", -nsf_fee))
    assert_that(account_balance(bank, destination.number)).is_equal_to(100)
