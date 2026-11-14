"""Compatibility entry point; implementation is in backtest/."""
from backtest.futures_open_to_night_close_convergence import *  # noqa: F401,F403
from backtest.futures_open_to_night_close_convergence import main

if __name__ == "__main__":
    main()
