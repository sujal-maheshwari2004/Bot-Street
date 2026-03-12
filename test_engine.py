import logging
logging.basicConfig(level=logging.INFO)
from engine.circuit_breaker import CircuitBreaker
from engine.candle_aggregator import CandleAggregator
from engine.trade_logger import TradeLogger

cb = CircuitBreaker()
ca = CandleAggregator()
tl = TradeLogger()

print('circuit_breaker halted PEAR:', cb.is_halted('PEAR'))
print('candle history PEAR:', ca.get_history('PEAR'))
print('✅ circuit_breaker + candle_aggregator + trade_logger OK')
