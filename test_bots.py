import logging
logging.basicConfig(level=logging.INFO)
from participants.market_maker import MarketMakerBot
from participants.momentum_bot import MomentumBot
from participants.random_bot import RandomBot
from participants.mean_reversion_bot import MeanReversionBot
from participants.base_bot import BotSymbolState

mm  = MarketMakerBot()
mob = MomentumBot()
rb  = RandomBot(seed=42)
mrb = MeanReversionBot()

state = BotSymbolState('PEAR', 150.0)
state.sentiment = 'bullish'
state.strength  = 0.8
state.rsi       = 72.0
state.vwap      = 147.0
state.ofi       = 0.6
state.ema_short = 151.0
state.ema_long  = 149.0

print('MarketMaker orders:', len(mm._on_tick('PEAR', state)))
print('Momentum orders:   ', mob._on_tick('PEAR', state))
print('Random orders:     ', rb._on_tick('PEAR', state))
mrb._portfolio.apply_buy('PEAR', 10, 148.0)
print('MeanRev orders:    ', mrb._on_tick('PEAR', state))
print('OK all bots OK')
