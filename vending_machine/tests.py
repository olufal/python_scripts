from handshake.vendingMachine import *
from handshake.money import *

dollar = Dollar(value=DollarAmount(2.00))
quarter = Quarter()
dime = Dime()

a = VendingMachine()
item = a.available_items[1]

a.select_item(item=item)

a.insert_bill(dollar)
a.insert_coin(quarter)
a.insert_coin(quarter)
a.insert_coin(dime)

a.dispense_item()