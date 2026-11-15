"""Install only journal tags, without changing packages or other tables."""
from common import db

def main():
    with db.get_connection() as conn:
        conn.execute("ALTER TABLE paper_trade.journals ADD COLUMN IF NOT EXISTS tags text[] NOT NULL DEFAULT ARRAY[]::text[]")
        conn.execute("SELECT tags FROM paper_trade.journals LIMIT 0")
        conn.commit()
    print('Journal tags schema ready. Existing journals preserved.')

if __name__ == '__main__':
    main()
