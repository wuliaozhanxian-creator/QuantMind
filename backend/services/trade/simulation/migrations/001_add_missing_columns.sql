-- Migration: Add missing columns to sim_trades table
-- Date: 2026-06-07
-- Reason: sync database with model backend/services/trade/simulation/models/trade.py

BEGIN;

-- Add trade_id (UUID, unique)
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS trade_id UUID DEFAULT gen_random_uuid();

-- Make trade_id unique if not already
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sim_trades_trade_id_key') THEN
        ALTER TABLE sim_trades ADD CONSTRAINT sim_trades_trade_id_key UNIQUE (trade_id);
    END IF;
END $$;

-- Add portfolio_id
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS portfolio_id INTEGER NOT NULL DEFAULT 0;

-- Add trading_mode
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS trading_mode VARCHAR(20) NOT NULL DEFAULT 'SIMULATION';

-- Add trade_value (required for stats query)
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS trade_value NUMERIC(18,4) NOT NULL DEFAULT 0;

-- Add stamp_duty
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS stamp_duty NUMERIC(18,4) NOT NULL DEFAULT 0;

-- Add transfer_fee
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS transfer_fee NUMERIC(18,4) NOT NULL DEFAULT 0;

-- Add total_fee
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS total_fee NUMERIC(18,4) NOT NULL DEFAULT 0;

-- Add executed_at (use trade_time as initial value)
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();

-- Update executed_at from trade_time where available
UPDATE sim_trades SET executed_at = trade_time WHERE executed_at IS NULL OR executed_at = NOW();

-- Add price_source
ALTER TABLE sim_trades
ADD COLUMN IF NOT EXISTS price_source VARCHAR(64);

-- Create indexes for new columns
CREATE INDEX IF NOT EXISTS idx_sim_trade_trade_id ON sim_trades(trade_id);
CREATE INDEX IF NOT EXISTS idx_sim_trade_portfolio_id ON sim_trades(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_sim_trade_trading_mode ON sim_trades(trading_mode);

-- Create composite index as defined in model
CREATE INDEX IF NOT EXISTS idx_sim_trade_tenant_user_symbol ON sim_trades(tenant_id, user_id, symbol);

-- Backfill trade_value from quantity * price
UPDATE sim_trades SET trade_value = quantity * price WHERE trade_value = 0;

COMMIT;

-- Verify migration
\d sim_trades