"""CLI entry point — fetch data and run a quick backtest from the terminal."""

import argparse
import json
from data.fetcher import fetch_and_store
from data.database import load_prices
from backtest.simulator import run_backtest


def main():
    parser = argparse.ArgumentParser(description="Trading Strategy CLI")
    sub = parser.add_subparsers(dest="cmd")

    fetch_p = sub.add_parser("fetch", help="Download and store historical data")
    fetch_p.add_argument("symbol", nargs="?", default="SPY")

    bt_p = sub.add_parser("backtest", help="Run a backtest with a rules JSON file")
    bt_p.add_argument("rules_file", help="Path to JSON file containing rules list")
    bt_p.add_argument("--symbol", default="SPY")
    bt_p.add_argument("--capital", type=float, default=10_000)
    bt_p.add_argument("--start", default=None)
    bt_p.add_argument("--end", default=None)

    args = parser.parse_args()

    if args.cmd == "fetch":
        fetch_and_store(args.symbol)

    elif args.cmd == "backtest":
        with open(args.rules_file) as f:
            rules = json.load(f)
        df = load_prices(args.symbol, start=args.start, end=args.end)
        if df.empty:
            print(f"No data for {args.symbol}. Run: python main.py fetch {args.symbol}")
            return
        result = run_backtest(df, rules, args.capital)
        print("\n=== Backtest Results ===")
        for k, v in result["metrics"].items():
            print(f"  {k}: {v}")
        print(f"  Final portfolio value: ${result['final_value']:,.2f}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
