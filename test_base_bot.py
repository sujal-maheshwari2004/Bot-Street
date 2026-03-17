import logging
logging.basicConfig(level=logging.INFO)
from participants.base_bot import BaseBot, BotSymbolState, BotPortfolio
from core.schemas import Order

class TestBot(BaseBot):
    def _on_tick(self, symbol, state):
        return [self._make_order(symbol, 'buy', 'limit', 5, state.price)]

bot = TestBot('test-bot', symbols=['PEAR'])
port = BotPortfolio()
port.apply_buy('PEAR', 10, 150.0)
port.apply_buy('PEAR', 10, 160.0)
print(f'Avg cost: {port.avg_cost}')
print(f'Position: {port.position("PEAR")}')
print(f'Cash: {port.cash}')
port.apply_sell('PEAR', 5, 170.0)
print(f'After sell position: {port.position("PEAR")}')
print('OK base_bot OK')
