"""Run once from the code directory with the existing project Python environment."""
from pathlib import Path
from common import db

if __name__=='__main__':
    sql=(Path(__file__).resolve().parent/'dashboard'/'sql'/'paper_trade.sql').read_text(encoding='utf-8')
    with db.get_connection() as conn:
        # The migration owns its transaction; execute on a fresh autocommit connection.
        conn.commit()
        conn.autocommit=True
        try:
            conn.execute(sql,prepare=False)
        except Exception:
            conn.execute('ROLLBACK')
            raise
    print('paper_trade schema ready. Existing market and backtest tables were not changed.')
