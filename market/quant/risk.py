"""
Risk Metrics — Pure Math, No Kafka, No Side Effects

Measures how much risk a participant is taking relative to their returns.
All functions operate on plain Python lists.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Realized Volatility    How much prices are moving
  Sharpe Ratio           Return per unit of risk
  Sortino Ratio          Sharpe but only penalizes downside risk
  Max Drawdown           Worst peak-to-trough loss
  VaR                    Value at Risk — worst expected loss
  CVaR                   Conditional VaR — expected loss beyond VaR
  Calmar Ratio           Return divided by max drawdown
─────────────────────────────────────────────────────────────────────────────
"""

import math
from dataclasses import dataclass
from typing import Optional

from config import SHARPE_RISK_FREE, VAR_CONFIDENCE


# ── Output dataclasses ────────────────────────────────────────────────────────

@dataclass
class RiskSnapshot:
    """
    Complete risk profile for one participant at one point in time.
    All fields Optional — None until enough trade history exists.
    """
    volatility    : Optional[float]   # annualized realized volatility
    sharpe        : Optional[float]   # Sharpe ratio
    sortino       : Optional[float]   # Sortino ratio
    max_drawdown  : Optional[float]   # worst peak-to-trough as fraction
    var_95        : Optional[float]   # Value at Risk 95%
    cvar_95       : Optional[float]   # Conditional VaR 95%
    calmar        : Optional[float]   # return / max_drawdown
    win_rate      : Optional[float]   # fraction of profitable trades
    avg_win       : Optional[float]   # average profit on winning trades
    avg_loss      : Optional[float]   # average loss on losing trades
    profit_factor : Optional[float]   # total_wins / total_losses


# ── Realized Volatility ───────────────────────────────────────────────────────

def realized_volatility(prices: list[float],
                        period: int = 30) -> Optional[float]:
    """
    Realized Volatility — standard deviation of log returns.

    log_return = ln(price_t / price_t-1)
    volatility = std_dev(log_returns) * sqrt(annualization_factor)

    Why log returns instead of simple returns?
      Log returns are additive across time and handle compounding correctly.
      Simple return of +50% then -50% = -25% net loss.
      Log return of +50% then -50% = 0% net (symmetrical).

    Annualization factor:
      Real markets: sqrt(252) for daily data (252 trading days/year)
      Our simulator: sqrt(trades_per_session) — we use sqrt(period)
      This normalizes volatility to a comparable scale.

    High volatility → wide Bollinger Bands, circuit breaker more likely
    Low volatility  → tight Bollinger Bands, Bollinger squeeze forming
    """
    if len(prices) < period + 1:
        return None

    window = prices[-(period + 1):]
    log_returns = [
        math.log(window[i] / window[i - 1])
        for i in range(1, len(window))
        if window[i - 1] > 0
    ]

    if len(log_returns) < 2:
        return None

    mean = sum(log_returns) / len(log_returns)
    variance = sum((r - mean) ** 2 for r in log_returns) / (len(log_returns) - 1)
    std = math.sqrt(variance)

    # annualize
    annualized = std * math.sqrt(period)
    return round(annualized, 6)


# ── Sharpe Ratio ──────────────────────────────────────────────────────────────

def sharpe_ratio(pnl_history: list[float],
                 risk_free: float = SHARPE_RISK_FREE) -> Optional[float]:
    """
    Sharpe Ratio — return per unit of total risk.

    Sharpe = (mean_return - risk_free_rate) / std_dev_returns

    Interpretation:
      < 0    → losing money vs holding cash
      0 - 1  → some return but not great risk-adjusted
      1 - 2  → good — acceptable for most strategies
      2 - 3  → very good
      > 3    → excellent (rare in real markets)

    Limitation: penalizes upside volatility equally as downside.
    A strategy with huge wins and small losses gets penalized.
    Sortino ratio fixes this.

    Requires at least 5 data points to be meaningful.
    """
    if len(pnl_history) < 5:
        return None

    mean = sum(pnl_history) / len(pnl_history)
    variance = sum((p - mean) ** 2 for p in pnl_history) / (len(pnl_history) - 1)
    std = math.sqrt(variance)

    if std == 0:
        return None

    return round((mean - risk_free) / std, 3)


# ── Sortino Ratio ─────────────────────────────────────────────────────────────

def sortino_ratio(pnl_history: list[float],
                  risk_free: float = SHARPE_RISK_FREE) -> Optional[float]:
    """
    Sortino Ratio — like Sharpe but only penalizes DOWNSIDE volatility.

    Sortino = (mean_return - risk_free) / downside_std_dev

    Downside std dev = std dev of negative returns only.

    Why better than Sharpe for asymmetric strategies?
      A strategy that wins big occasionally and loses small often
      has high upside volatility but low downside volatility.
      Sharpe penalizes the big wins. Sortino does not.

    Higher Sortino vs Sharpe → strategy has positive skew (good)
    Lower Sortino vs Sharpe  → strategy has negative skew (bad)
    """
    if len(pnl_history) < 5:
        return None

    mean = sum(pnl_history) / len(pnl_history)
    downside = [p for p in pnl_history if p < risk_free]

    if len(downside) < 2:
        return None

    downside_variance = sum(p ** 2 for p in downside) / len(downside)
    downside_std = math.sqrt(downside_variance)

    if downside_std == 0:
        return None

    return round((mean - risk_free) / downside_std, 3)


# ── Max Drawdown ──────────────────────────────────────────────────────────────

def max_drawdown(portfolio_values: list[float]) -> Optional[float]:
    """
    Maximum Drawdown — largest peak-to-trough decline.

    For each point in time:
      peak     = highest portfolio value seen so far
      drawdown = (peak - current) / peak

    Max drawdown = worst drawdown ever experienced.

    Interpretation:
      0.10 = 10% drawdown at worst point
      0.50 = lost half of peak value at some point (catastrophic)

    Key risk metric for strategy evaluation:
      High Sharpe + Low Max Drawdown = great strategy
      High Sharpe + High Max Drawdown = fragile — will blow up eventually

    Warren Buffett: "Rule #1: Never lose money.
                    Rule #2: Never forget Rule #1."
    """
    if len(portfolio_values) < 2:
        return None

    peak = portfolio_values[0]
    max_dd = 0.0

    for value in portfolio_values:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak
            max_dd = max(max_dd, dd)

    return round(max_dd, 6)


# ── Value at Risk ─────────────────────────────────────────────────────────────

def var(pnl_history: list[float],
        confidence: float = VAR_CONFIDENCE) -> Optional[float]:
    """
    Value at Risk (Historical VaR)

    "With X% confidence, I will not lose more than $Y in one period."

    Historical method:
      Sort all past P&Ls from worst to best.
      VaR at 95% = the value at the 5th percentile.

    Example:
      100 trades, sorted P&Ls: [-$50, -$40, -$35, -$30, -$25, ...]
      VaR(95%) = -$35 (5th worst outcome out of 100)
      Meaning: 95% of the time, loss won't exceed $35

    VaR is widely used but has known limitations:
      - Assumes past distribution predicts future
      - Does not tell you HOW BAD losses beyond VaR can get
      - CVaR fixes the second limitation

    Negative return = loss (expected for VaR)
    """
    if len(pnl_history) < 10:
        return None

    sorted_pnl = sorted(pnl_history)
    idx = int(len(sorted_pnl) * (1 - confidence))
    idx = max(0, min(idx, len(sorted_pnl) - 1))
    return round(sorted_pnl[idx], 2)


# ── Conditional VaR (CVaR / Expected Shortfall) ───────────────────────────────

def cvar(pnl_history: list[float],
         confidence: float = VAR_CONFIDENCE) -> Optional[float]:
    """
    Conditional Value at Risk (CVaR) — also called Expected Shortfall.

    "Given that I AM in the worst X% of outcomes, what is my average loss?"

    CVaR = mean of all losses worse than VaR threshold

    CVaR > VaR (in absolute terms) — always worse than VaR by definition.

    Why CVaR matters:
      VaR: "95% of the time loss < $35"
      CVaR: "But when things ARE bad, average loss is $48"

      During 2008 financial crisis, many models had acceptable VaR
      but catastrophic CVaR — they didn't model tail risk properly.

    CVaR is now preferred by regulators over VaR.
    """
    if len(pnl_history) < 10:
        return None

    sorted_pnl = sorted(pnl_history)
    cutoff_idx = int(len(sorted_pnl) * (1 - confidence))
    cutoff_idx = max(1, cutoff_idx)
    tail = sorted_pnl[:cutoff_idx]

    if not tail:
        return None

    return round(sum(tail) / len(tail), 2)


# ── Calmar Ratio ──────────────────────────────────────────────────────────────

def calmar_ratio(total_return: float,
                 max_dd: float) -> Optional[float]:
    """
    Calmar Ratio = annualized_return / max_drawdown

    Measures return relative to the worst loss experienced.
    Higher = better (more return per unit of drawdown pain).

    Calmar > 3  → excellent
    Calmar 1-3  → acceptable
    Calmar < 1  → drawdown is bigger than your return (bad)

    Popular with hedge funds — complements Sharpe by focusing
    on tail risk rather than average volatility.
    """
    if max_dd is None or max_dd == 0:
        return None
    return round(total_return / max_dd, 3)


# ── Trade statistics ──────────────────────────────────────────────────────────

def trade_stats(pnl_history: list[float]) -> dict:
    """
    Basic trade statistics — win rate, avg win/loss, profit factor.

    Win rate = wins / total_trades
      > 50% = more winners than losers
      A strategy can be profitable with <50% win rate
      if average win >> average loss (positive expectancy)

    Profit factor = total_profit / total_loss
      > 1.0 = profitable overall
      > 2.0 = very good
      < 1.0 = losing strategy

    Expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)
      Positive expectancy = profitable in long run
      This is the ONLY metric that truly matters for a strategy.
    """
    if not pnl_history:
        return {}

    wins  = [p for p in pnl_history if p > 0]
    losses = [p for p in pnl_history if p < 0]

    win_rate     = len(wins) / len(pnl_history) if pnl_history else None
    avg_win      = sum(wins) / len(wins)   if wins   else None
    avg_loss     = sum(losses) / len(losses) if losses else None
    total_profit = sum(wins)
    total_loss   = abs(sum(losses))
    profit_factor = round(total_profit / total_loss, 3) if total_loss > 0 else None

    expectancy = None
    if win_rate is not None and avg_win is not None and avg_loss is not None:
        expectancy = round(
            (win_rate * avg_win) + ((1 - win_rate) * avg_loss), 2
        )

    return {
        "total_trades"  : len(pnl_history),
        "wins"          : len(wins),
        "losses"        : len(losses),
        "win_rate"      : round(win_rate, 3) if win_rate else None,
        "avg_win"       : round(avg_win, 2)  if avg_win  else None,
        "avg_loss"      : round(avg_loss, 2) if avg_loss else None,
        "profit_factor" : profit_factor,
        "expectancy"    : expectancy,
    }


# ── Risk Engine ───────────────────────────────────────────────────────────────

class RiskEngine:
    """
    Stateful risk calculator for one participant.
    Maintains rolling P&L history and portfolio value history.

    Usage:
        engine = RiskEngine('user')
        engine.record_trade(pnl=75.0)
        engine.record_portfolio_value(10500.0)
        snapshot = engine.snapshot()
    """

    def __init__(self, client_id: str):
        self.client_id         = client_id
        self._pnl_history      : list[float] = []
        self._portfolio_values : list[float] = []
        self._price_history    : list[float] = []

    def record_trade(self, pnl: float):
        """Call after every closed trade."""
        self._pnl_history.append(pnl)
        if len(self._pnl_history) > 500:
            self._pnl_history.pop(0)

    def record_portfolio_value(self, value: float):
        """Call after every portfolio update."""
        self._portfolio_values.append(value)
        if len(self._portfolio_values) > 500:
            self._portfolio_values.pop(0)

    def record_price(self, price: float):
        """Call on every price update for volatility calc."""
        self._price_history.append(price)
        if len(self._price_history) > 500:
            self._price_history.pop(0)

    def snapshot(self) -> RiskSnapshot:
        total_return = (
            sum(self._pnl_history) /
            self._portfolio_values[0]
            if self._portfolio_values else 0
        )
        max_dd = max_drawdown(self._portfolio_values)

        return RiskSnapshot(
            volatility   = realized_volatility(self._price_history),
            sharpe       = sharpe_ratio(self._pnl_history),
            sortino      = sortino_ratio(self._pnl_history),
            max_drawdown = max_dd,
            var_95       = var(self._pnl_history),
            cvar_95      = cvar(self._pnl_history),
            calmar       = calmar_ratio(total_return, max_dd),
            win_rate     = trade_stats(self._pnl_history).get("win_rate"),
            avg_win      = trade_stats(self._pnl_history).get("avg_win"),
            avg_loss     = trade_stats(self._pnl_history).get("avg_loss"),
            profit_factor = trade_stats(self._pnl_history).get("profit_factor"),
        )