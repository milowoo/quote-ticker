package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// TableManager auto-creates per-symbol kline tables.
type TableManager struct {
	db    *sql.DB
	cache sync.Map // map[symbolLower]bool
}

func NewTableManager(db *sql.DB) *TableManager {
	return &TableManager{db: db}
}

// EnsureTable creates the kline table for symbol if it does not exist.
func (m *TableManager) EnsureTable(ctx context.Context, symbol string) error {
	tableName := TableName(symbol)
	if _, ok := m.cache.Load(tableName); ok {
		return nil
	}

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		iv     VARCHAR(8)      NOT NULL COMMENT 'interval',
		st     BIGINT          NOT NULL COMMENT 'start_time(ms)',

		ct     BIGINT          NOT NULL COMMENT 'close_time(ms)',
		o      DECIMAL(40,20)  NOT NULL COMMENT 'open',
		h      DECIMAL(40,20)  NOT NULL COMMENT 'high',
		l      DECIMAL(40,20)  NOT NULL COMMENT 'low',
		c      DECIMAL(40,20)  NOT NULL COMMENT 'close',
		v      DECIMAL(40,20)  NOT NULL DEFAULT 0 COMMENT 'volume',
		q      DECIMAL(40,20)  NOT NULL DEFAULT 0 COMMENT 'amount',
		n      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT 'trade_count',
		bv     DECIMAL(40,20)  NOT NULL DEFAULT 0 COMMENT 'buy_taker_vol',
		bq     DECIMAL(40,20)  NOT NULL DEFAULT 0 COMMENT 'buy_taker_amt',
		wavg   DECIMAL(40,20)  NOT NULL DEFAULT 0 COMMENT 'weighted_avg',
		created_at BIGINT      NOT NULL DEFAULT 0,
		updated_at BIGINT      NOT NULL DEFAULT 0,

		PRIMARY KEY (iv, st) CLUSTERED
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, tableName)

	if _, err := m.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create table %s: %w", tableName, err)
	}

	m.cache.Store(tableName, true)
	return nil
}

// TableName returns the table name for a symbol.
func TableName(symbol string) string {
	return "t_kline_" + strings.ToLower(symbol)
}

// ListSymbols discovers all kline tables from information_schema and
// returns the symbol names. Used by the continuity checker.
func (m *TableManager) ListSymbols(ctx context.Context) ([]string, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT TABLE_NAME FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 't_kline_%'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		if len(tableName) > 8 {
			out = append(out, strings.ToUpper(tableName[8:]))
			m.cache.Store(tableName, true) // warm cache
		}
	}
	return out, rows.Err()
}

// NeededTables returns all cached table names (for metrics / admin).
func (m *TableManager) NeededTables() []string {
	var out []string
	m.cache.Range(func(key, _ interface{}) bool {
		out = append(out, key.(string))
		return true
	})
	return out
}
