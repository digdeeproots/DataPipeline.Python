from enum import StrEnum


class Currency(StrEnum):
    Dollars = "USD"
    Pesos = "MXN"
    Baht = "THB"
    Ringgit = "MYR"
    Yuan = "CNY"

class Account:
    def __init__(self, starting_balance, currency):
        self.currency = currency
        self._starting_balance = starting_balance
        self._committed_transactions = []

    @property
    def balance(self):
        return self._starting_balance + sum([t.amount for t in self._committed_transactions])


class Loan(Account):
    def __init__(self, starting_balance):
        super(Loan, self).__init__(starting_balance)

    @property
    def balance(self):
        return -super(Loan, self).balance()


class TransactionHalf:
    def __init__(self, amount: float, account: Account):
        self.amount = amount
        self.account = account


class Transaction:
    def __init__(self, source: TransactionHalf, dest: TransactionHalf):
        self.source = source
        self.dest = dest


def move_money():
    pass
