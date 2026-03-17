import logging
logging.basicConfig(level=logging.WARNING)
from user.cli import UserSession
from config import SYMBOL_LIST, SYMBOLS

session = UserSession()
session.start_listeners()

print('Session initialised')
print('Prices:', {s: SYMBOLS[s][1] for s in SYMBOL_LIST})
session.show_prices()
session.stop()
print('OK cli OK')
