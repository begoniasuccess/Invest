"""Shared TX minute-bar contract selection used by charts and turning reports.

The input CTE must be named ``tx_contract_candidates`` and contain:
``bar_datetime, trading_date, trading_session, contract_date, volume`` plus
the OHLC columns needed by its consumer.

Selection is performed once for the whole trading date/session.  It first
uses the same front-month calendar rule as the daily futures report; if that
contract is unavailable, it falls back to the session's largest total volume.
This intentionally avoids switching contracts minute by minute.
"""

TX_FIXED_CONTRACT_CTES = r"""
, tx_contract_totals AS (
    SELECT
        trading_date,
        trading_session,
        contract_date,
        SUM(COALESCE(volume, 0)) AS session_volume
    FROM tx_contract_candidates
    GROUP BY trading_date, trading_session, contract_date
), tx_contract_ranked AS (
    SELECT
        totals.*,
        front.contract_month AS calendar_front_month,
        ROW_NUMBER() OVER (
            PARTITION BY totals.trading_date, totals.trading_session
            ORDER BY
                CASE
                    WHEN front.contract_month IS NOT NULL
                     AND totals.contract_date = front.contract_month THEN 0
                    ELSE 1
                END,
                totals.session_volume DESC,
                totals.contract_date ASC
        ) AS contract_rank
    FROM tx_contract_totals totals
    LEFT JOIN LATERAL (
        SELECT settlement.contract_month
        FROM fm_taiwan_futures_final_settlement_price settlement
        WHERE settlement.futures_id = 'TX'
          AND settlement.futures_type = 'index'
          AND settlement.contract_month NOT LIKE '%W%'
          AND LENGTH(settlement.contract_month) = 6
          AND settlement.date >= totals.trading_date
        ORDER BY settlement.date ASC
        LIMIT 1
    ) front ON TRUE
), tx_selected_contracts AS (
    SELECT trading_date, trading_session, contract_date
    FROM tx_contract_ranked
    WHERE contract_rank = 1
), tx_selected_minutes AS (
    SELECT candidates.*
    FROM tx_contract_candidates candidates
    JOIN tx_selected_contracts selected
      ON selected.trading_date = candidates.trading_date
     AND selected.trading_session = candidates.trading_session
     AND selected.contract_date = candidates.contract_date
)
"""
