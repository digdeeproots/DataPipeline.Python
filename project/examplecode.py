from enum import Enum
from typing import List


class Currency(Enum):
    Dollars = "USD"
    Pesos = "MXN"
    Baht = "THB"
    Ringgit = "MYR"
    Yuan = "CNY"


class StandardAccounts(Enum):
    Equity = "Shareholder Equity"
    Cash = "Cash"


class ChangeInAccountValue:
    def __init__(self, account: str, amount: int):
        self.amount = amount
        self.account = account


class JournalEntry:
    def __init__(self, credits: List[ChangeInAccountValue], debits: List[ChangeInAccountValue]):
        self.credits: List[ChangeInAccountValue] = credits
        self.debits: List[ChangeInAccountValue] = debits
        assert sum(c.amount for c in self.credits) - sum(d.amount for d in self.debits) == 0


class Journal:
    def __init__(self, currency: Currency):
        self.currency = currency
        self.entries: List[JournalEntry] = []


def account_balance(bank, account):
    return sum(sum(c.amount for c in e.credits if c.account == account) - sum(
        d.amount for d in e.debits if d.account == account) for e in bank.entries)


def start_bank(initial_investment: int, currency: Currency):
    bank = Journal(currency)
    bank.entries.append(JournalEntry(
        [ChangeInAccountValue(StandardAccounts.Cash, initial_investment)],
        [ChangeInAccountValue(StandardAccounts.Equity, initial_investment)]))
    return bank


def start_currency_exchange(initial_funds):
    return {funding.currency: start_bank(funding.amount, funding.currency) for funding in initial_funds}
