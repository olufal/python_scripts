"""
Author: John Falope
Contain class implementations for items and vending machine
TODO - add functionality for adding different items in various sections
TODO - add database functionality for inventory purposes
"""
from handshake.money import *

COINS = [Dollar(value=DollarAmount(1.00)),  Quarter(), Dime(), Nickel(), Penny()]


class Item:
    def __init__(self, name: str, price: DollarAmount):
        self.name = name
        self.price = price

    def show(self):
        print(f'{self.name} - ${self.price}')


class VendingMachine:
    """
    A virtual vending machine.
    """
    def __init__(self):
        self.inserted_amount = []
        self.selected_item = None
        self.current_balance = None
        self.remaining_change = []
        self.available_items = [
            Item(name='Lays', price=DollarAmount('2.10')),
            Item(name='Nerds', price=DollarAmount('2.59')),
            Item(name='Coke', price=DollarAmount('3.99')),
        ]


    def select_item(self, item: Item):
        self.selected_item = item
        try:
            if self.selected_item in self.available_items:
                self.selected_item.show()
                print('Please insert bills or coins as payment')
        except ValueError:
            print(f'Selection not available')
            print('Here are the available items')
            for i in self.available_items:
                i.show()


    def insert_coin(self, coin):
        """
        Accepts a Coin instance and inserts it into the vending machine.
        """
        if not isinstance(coin, Coin):
            raise ValueError()

        self.inserted_amount.append(coin.value)
        print(f'Inserted ${coin.value}')

    def insert_bill(self, bill):
        """
        Accepts a bill instance and inserts it into the vending machine.
        """
        if not isinstance(bill, Dollar):
            raise ValueError()

        self.inserted_amount.append(bill.value)
        print(f'Inserted ${bill.value}')


    def get_balance(self):
        """
        Returns the balance remaining.
        """
        balance = round(sum(self.inserted_amount), 2)
        return balance


    def get_change(self):
        """
        Calculate the coins required to make change equal to amount.
        """
        remainder = self.current_balance - self.selected_item.price
        while remainder > 0:
            print(f'Processing remaining change - ${remainder}')
            amounts = [coin.value for coin in COINS]
            counts = [0 for _ in COINS]
            for index, amount in enumerate(amounts):
                counts[index] = remainder // amount
                remainder %= amount
            change_list = [(int(count), coin.value) for count, coin in zip(counts, COINS) if count]
            return change_list

    def dispense_item(self):
        """
        main method that drives the purchase
        """
        self.current_balance = self.get_balance()
        print(f'Current balance = ${self.current_balance}')

        if self.selected_item is not None and self.current_balance - self.selected_item.price > 0:
            try:
                index = self.available_items.index(self.selected_item)
                self.available_items.pop(index)
                print(f'Please collect {self.selected_item.name}')

                self.remaining_change = self.get_change()
                for i, j in self.remaining_change:
                    print(f'Your Change is {i} - ${j}')

                self.selected_item = None
                self.remaining_change = []
            except ValueError as v:
                print(v)

        else:
            print(f'You have insufficient payment to purchase this item')


