import json
import sys
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from format_levels import format_levels

class TestLevels(unittest.TestCase):
    def test_taiex(self):
        data = json.loads((ROOT / 'examples/taiex.json').read_text(encoding='utf-8'))
        lines = format_levels(data['close'], data['records']).splitlines()
        self.assertEqual(len(lines), 15)
        self.assertEqual(lines[0], '46415 60MA (+16.91%)')
        self.assertIn('39702 今日收盤（基準） (0.00%)', lines)
        self.assertEqual(lines[-1], '37837 今日最低 (-4.70%)')

    def test_tx(self):
        data = json.loads((ROOT / 'examples/tx.json').read_text(encoding='utf-8'))
        lines = format_levels(data['close'], data['records']).splitlines()
        self.assertEqual(len(lines), 25)
        self.assertIn('37694 全日最低 / 日盤低 (-4.94%)', lines)
        self.assertIn('39652 全日收盤（基準） (0.00%)', lines)

    def test_close_merge(self):
        self.assertEqual(format_levels(100, [{'price':100,'label':'全日收盤'}, {'price':100,'label':'夜盤開'}]), '100 全日收盤 / 夜盤開（基準） (0.00%)')

if __name__ == '__main__':
    unittest.main()
