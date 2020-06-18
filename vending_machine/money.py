"""
Author: John Falope
Contains all models pertaining to money, coins, etc.
"""
from decimal import Decimal

class DollarAmount(Decimal):
    """
    Represents a dollar amount.
    Extends the decimal.Decimal class.
    """
    def __repr__(self):
        return f"DollarAmount('{self}')"

    def __str__(self):
        return f'${self:,.2f}'

class Coin:
    """Base class representing coins."""
    value = DollarAmount('0')

    def __radd__(self, other):
        return self.value + other

    def __eq__(self, other):
        return self.value == other.value


class Penny(Coin):
    value =  DollarAmount('0.01')


class Nickel(Coin):
    value = DollarAmount('0.05')


class Dime(Coin):
    value = DollarAmount('0.10')


class Quarter(Coin):
    value = DollarAmount('0.25')


class Dollar:
    def __init__(self, value: DollarAmount):
        self.value = value
