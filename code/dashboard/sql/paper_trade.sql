BEGIN;
CREATE SCHEMA IF NOT EXISTS paper_trade;
CREATE TABLE IF NOT EXISTS paper_trade.accounts (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 name text NOT NULL UNIQUE CHECK(length(trim(name)) BETWEEN 1 AND 80),
 initial_equity numeric(18,2) NOT NULL DEFAULT 1000000 CHECK(initial_equity > 0),
 fee_tx numeric(12,2) NOT NULL DEFAULT 50 CHECK(fee_tx >= 0),
 fee_mtx numeric(12,2) NOT NULL DEFAULT 30 CHECK(fee_mtx >= 0),
 fee_tmf numeric(12,2) NOT NULL DEFAULT 15 CHECK(fee_tmf >= 0),
 slippage integer NOT NULL DEFAULT 0 CHECK(slippage BETWEEN 0 AND 1000),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS paper_trade.runs (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 account_id bigint NOT NULL REFERENCES paper_trade.accounts(id),
 market text NOT NULL CHECK(market IN ('tx_day','tx_night')),
 contract text NOT NULL CHECK(contract ~ '^[0-9]{6}$'),
 start_at timestamp NOT NULL, end_at timestamp NOT NULL CHECK(end_at > start_at),
 cursor_time timestamp, cursor_row bigint, cursor_id bigint,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK ((cursor_time IS NULL AND cursor_row IS NULL AND cursor_id IS NULL) OR (cursor_time IS NOT NULL AND cursor_row IS NOT NULL AND cursor_id IS NOT NULL AND cursor_time BETWEEN start_at AND end_at))
);
CREATE TABLE IF NOT EXISTS paper_trade.orders (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 run_id bigint NOT NULL REFERENCES paper_trade.runs(id),
 request_id uuid NOT NULL UNIQUE,
 product text NOT NULL CHECK(product IN ('TX','MTX','TMF')),
 direction text NOT NULL CHECK(direction IN ('LONG','SHORT')),
 quantity integer NOT NULL CHECK(quantity BETWEEN 1 AND 1000),
 order_type text NOT NULL CHECK(order_type IN ('MARKET','LIMIT','STOP')),
 target numeric(18,4), stop_loss numeric(18,4), take_profit numeric(18,4),
 status text NOT NULL CHECK(status IN ('PENDING','FILLED','CANCELLED')),
 submitted_at timestamp NOT NULL, submitted_tick_id bigint NOT NULL,
 submitted_clock_at timestamp NOT NULL,
 submitted_client_at timestamptz NOT NULL, submitted_server_at timestamptz NOT NULL DEFAULT now(),
 fee_tx numeric(12,2) NOT NULL, fee_mtx numeric(12,2) NOT NULL, fee_tmf numeric(12,2) NOT NULL,
 slippage integer NOT NULL, tax_rate numeric(12,8) NOT NULL DEFAULT 0.00002,
 cancelled_at timestamp, cancelled_client_at timestamptz,
 CHECK ((order_type='MARKET' AND target IS NULL) OR (order_type<>'MARKET' AND target IS NOT NULL AND target>0)),
 CHECK (stop_loss IS NULL OR stop_loss>0), CHECK (take_profit IS NULL OR take_profit>0),
 CHECK (submitted_clock_at>=submitted_at),
 CHECK ((status='CANCELLED' AND cancelled_at IS NOT NULL AND cancelled_client_at IS NOT NULL) OR (status<>'CANCELLED' AND cancelled_at IS NULL AND cancelled_client_at IS NULL))
);
CREATE TABLE IF NOT EXISTS paper_trade.trades (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 order_id bigint NOT NULL UNIQUE REFERENCES paper_trade.orders(id) ON DELETE CASCADE,
 entry_time timestamp NOT NULL, entry_tick_id bigint NOT NULL,
 entry_price numeric(18,4) NOT NULL CHECK(entry_price>0), entry_raw_price numeric(18,4) NOT NULL CHECK(entry_raw_price>0),
 entry_client_at timestamptz NOT NULL, entry_server_at timestamptz NOT NULL DEFAULT now(),
 exit_time timestamp, exit_tick_id bigint, exit_price numeric(18,4), exit_raw_price numeric(18,4),
 exit_client_at timestamptz, exit_server_at timestamptz, exit_reason text,
 note text NOT NULL DEFAULT '' CHECK(length(note)<=10000), note_updated_at timestamptz,
 CHECK ((exit_time IS NULL AND exit_tick_id IS NULL AND exit_price IS NULL AND exit_raw_price IS NULL AND exit_client_at IS NULL AND exit_server_at IS NULL AND exit_reason IS NULL) OR
 (exit_time IS NOT NULL AND exit_time>=entry_time AND exit_tick_id IS NOT NULL AND exit_price IS NOT NULL AND exit_price>0 AND exit_raw_price IS NOT NULL AND exit_raw_price>0 AND exit_client_at IS NOT NULL AND exit_server_at IS NOT NULL AND exit_reason IS NOT NULL AND exit_reason IN ('MANUAL','STOP_LOSS','TAKE_PROFIT')))
);
-- Position effect migration: legacy orders remain explicit new positions.
ALTER TABLE paper_trade.orders ADD COLUMN IF NOT EXISTS position_effect text NOT NULL DEFAULT 'OPEN' CHECK(position_effect IN ('AUTO','OPEN','CLOSE'));
ALTER TABLE paper_trade.orders ADD COLUMN IF NOT EXISTS cancel_reason text;
-- Backfill legacy semantics before reinstalling the immutability trigger.
DROP TRIGGER IF EXISTS guard_order_update ON paper_trade.orders;
ALTER TABLE paper_trade.orders ADD COLUMN IF NOT EXISTS time_in_force text;
UPDATE paper_trade.orders SET time_in_force=CASE WHEN order_type='LIMIT' THEN 'ROD' ELSE 'IOC' END WHERE time_in_force IS NULL;
ALTER TABLE paper_trade.orders ALTER COLUMN time_in_force SET DEFAULT 'ROD';
ALTER TABLE paper_trade.orders ALTER COLUMN time_in_force SET NOT NULL;
ALTER TABLE paper_trade.orders DROP CONSTRAINT IF EXISTS paper_orders_tif_check;
ALTER TABLE paper_trade.orders ADD CONSTRAINT paper_orders_tif_check CHECK(time_in_force IN ('ROD','IOC','FOK') AND (order_type='LIMIT' OR time_in_force<>'ROD'));

ALTER TABLE paper_trade.trades ADD COLUMN IF NOT EXISTS position_quantity integer CHECK(position_quantity > 0);
ALTER TABLE paper_trade.trades ADD COLUMN IF NOT EXISTS exit_order_id bigint REFERENCES paper_trade.orders(id);
ALTER TABLE paper_trade.trades DROP CONSTRAINT IF EXISTS trades_order_id_key;
CREATE INDEX IF NOT EXISTS paper_trades_order_idx ON paper_trade.trades(order_id);
CREATE INDEX IF NOT EXISTS paper_runs_account_idx ON paper_trade.runs(account_id, id DESC);
CREATE INDEX IF NOT EXISTS paper_orders_run_idx ON paper_trade.orders(run_id,status,id);
-- Prevent changes to fixed execution facts, even through an accidental UPDATE.
CREATE OR REPLACE FUNCTION paper_trade.guard_order_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['status','cancelled_at','cancelled_client_at','cancel_reason']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['status','cancelled_at','cancelled_client_at','cancel_reason']) THEN
  RAISE EXCEPTION 'Immutable simulated order details';
 END IF;
 IF OLD.status <> 'PENDING' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'Finalized simulated order cannot be changed'; END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_order_update ON paper_trade.orders;
CREATE TRIGGER guard_order_update BEFORE UPDATE ON paper_trade.orders FOR EACH ROW EXECUTE FUNCTION paper_trade.guard_order_update();
CREATE OR REPLACE FUNCTION paper_trade.guard_trade_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (to_jsonb(NEW)-ARRAY['note','note_updated_at','position_quantity','exit_order_id','exit_time','exit_tick_id','exit_price','exit_raw_price','exit_client_at','exit_server_at','exit_reason']) IS DISTINCT FROM
    (to_jsonb(OLD)-ARRAY['note','note_updated_at','position_quantity','exit_order_id','exit_time','exit_tick_id','exit_price','exit_raw_price','exit_client_at','exit_server_at','exit_reason']) THEN
  RAISE EXCEPTION 'Immutable simulated entry details';
 END IF;
 IF NEW.position_quantity IS DISTINCT FROM OLD.position_quantity AND (OLD.exit_time IS NOT NULL OR NEW.position_quantity IS NULL OR NEW.position_quantity >= COALESCE(OLD.position_quantity,(SELECT quantity FROM paper_trade.orders WHERE id=OLD.order_id))) THEN
  RAISE EXCEPTION 'Position quantity may only decrease during a split';
 END IF;
 IF OLD.exit_time IS NOT NULL AND (to_jsonb(NEW)-ARRAY['note','note_updated_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['note','note_updated_at']) THEN
  RAISE EXCEPTION 'Finalized simulated trade cannot be changed';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_trade_update ON paper_trade.trades;
CREATE TRIGGER guard_trade_update BEFORE UPDATE ON paper_trade.trades FOR EACH ROW EXECUTE FUNCTION paper_trade.guard_trade_update();
COMMIT;
