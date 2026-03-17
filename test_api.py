import logging
logging.basicConfig(level=logging.WARNING)
from api.models import PlaceOrderRequest, OrderResponse
from api.routes import orders, market, portfolio, market_status
print('models OK')
print('routes OK')

from api.main import app
print('app OK')
print('OK api OK')
