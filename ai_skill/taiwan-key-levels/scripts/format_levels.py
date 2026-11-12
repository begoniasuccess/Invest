#!/usr/bin/env python3
"""Sort and format Taiwan stock/futures key levels. Input: JSON with close and records."""
import argparse
import json
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path


def format_levels(close, records):
    base = Decimal(str(close).replace(',', ''))
    if base <= 0:
        raise ValueError('close must be positive')
    grouped = defaultdict(list)
    for record in records:
        price = Decimal(str(record['price']).replace(',', ''))
        label = str(record['label']).strip()
        if not label:
            raise ValueError('record label must not be blank')
        if label not in grouped[price]:
            grouped[price].append(label)
    if base not in grouped:
        raise ValueError('base close must be present as a labeled record')
    lines = []
    for price in sorted(grouped, reverse=True):
        pct = ((price - base) / base * Decimal(100)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        if pct == 0:
            percent = '0.00%'
        else:
            percent = f'{pct:+.2f}%'
        price_str = format(price, 'f').rstrip('0').rstrip('.') if price % 1 else str(int(price))
        labels = ' / '.join(grouped[price])
        if price == base:
            labels += '（基準）'
        lines.append(f'{price_str} {labels} ({percent})')
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i', type=Path, help='JSON file; otherwise stdin')
    args = parser.parse_args()
    import sys
    data = json.loads(args.input.read_text(encoding='utf-8') if args.input else sys.stdin.read())
    print(format_levels(data['close'], data['records']))


if __name__ == '__main__':
    main()
