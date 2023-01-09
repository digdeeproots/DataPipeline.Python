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
    Deposits = "Customer Deposits"
    Loans = "Loan Principal Outstanding"


_next_account_number = 0

class ChangeInAccountValue:
    def __init__(self, account: str, amount: int):
        self.amount = amount
        self.account = account

    def __repr__(self):
        return f"{self.account}|{self.amount}"


class JournalEntry:
    def __init__(self, credits: List[ChangeInAccountValue], debits: List[ChangeInAccountValue], reason: str):
        self.reason = reason
        self.credits: List[ChangeInAccountValue] = credits
        self.debits: List[ChangeInAccountValue] = debits
        assert sum(c.amount for c in self.credits) - sum(d.amount for d in self.debits) == 0

    def __repr__(self):
        return f"Credits: {repr(self.credits)}, Debits: {repr(self.debits)} for {self.reason}"


class Journal:
    def __init__(self, currency: Currency):
        self.currency = currency
        self.entries: List[JournalEntry] = []

    def __repr__(self):
        entries = "\n\t".join(repr(e) for e in self.entries)
        return f"Bank in {self.currency} with journal\n\t{entries}"


class Account:
    def __init__(self, bank: Journal, number):
        self.number = number
        self.bank: Journal = bank

    def __repr__(self):
        return f"Account {self.number}"

    def deposit(self, amount):
        self.bank.entries.append(JournalEntry([ChangeInAccountValue(self.number, amount)],
                                              [ChangeInAccountValue(StandardAccounts.Deposits, amount)], "Deposit"))


class LogEntry:
    def __init__(self, credit, debit, reason):
        self.amount = (credit or 0) - (debit or 0)
        self.reason = reason

    def __repr__(self):
        return f"{self.reason}: {self.amount}"


def account_balance(bank, account):
    return sum(sum(c.amount for c in e.credits if c.account == account) - sum(
        d.amount for d in e.debits if d.account == account) for e in bank.entries)


def account_log(bank, account):
    matches = [(next((c.amount for c in e.credits if c.account == account), None), next((d.amount for d in e.debits if d.account == account), None), e.reason) for e in bank.entries]
    return [LogEntry(*m) for m in matches if m[0] is not None or m[1] is not None]


def start_bank(initial_investment: int, currency: Currency):
    bank = Journal(currency)
    bank.entries.append(JournalEntry(
        [ChangeInAccountValue(StandardAccounts.Cash, initial_investment)],
        [ChangeInAccountValue(StandardAccounts.Equity, initial_investment)], "Initial investment"))
    return bank


def start_currency_exchange(initial_funds):
    return {funding.currency: start_bank(funding.amount, funding.currency) for funding in initial_funds}


def transfer(source: Account, destination: Account, amount: int, nsf_fee=0, using_feature_new_transfers=False):
    if using_feature_new_transfers:
        transfer_old(source, destination, amount, nsf_fee)
    else:
        transfer_old(source, destination, amount, nsf_fee)


def transfer_old(source, destination, amount, nsf_fee=0):
    if account_balance(source.bank, source.number) >= amount:
        source.bank.entries.append(JournalEntry([ChangeInAccountValue(destination.number, amount)],
                                                [ChangeInAccountValue(source.number, amount)], "Transfer"))
    elif nsf_fee > 0:
        source.bank.entries.append(JournalEntry(
            [ChangeInAccountValue(StandardAccounts.Cash, nsf_fee)],
            [ChangeInAccountValue(source.number, nsf_fee)], "Insufficient funds for transfer"))


def transfer_new(source, destination, amount, nsf_fee=0):
    if account_balance(source.bank, source.number) >= amount:
        source.bank.entries.append(JournalEntry([ChangeInAccountValue(destination.number, amount)],
                                                [ChangeInAccountValue(source.number, amount)], "Transfer"))
    elif nsf_fee > 0:
        source.bank.entries.append(JournalEntry(
            [ChangeInAccountValue(StandardAccounts.Cash, nsf_fee)],
            [ChangeInAccountValue(source.number, nsf_fee)], "Insufficient funds for transfer"))


def create_account(bank):
    global _next_account_number
    _next_account_number += 1
    return Account(bank, _next_account_number)
