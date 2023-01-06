from  assertpy import assert_that
from project.examplecode import start_bank, start_currency_exchange, account_balance, Currency, StandardAccounts

def test_outer_fun_does_magic():
    bank = start_bank(10000, Currency.Dollars)
    assert_that(account_balance(bank, StandardAccounts.Cash)).is_equal_to(10000)
