package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"quote-ticker/internal/decimal"
	"quote-ticker/internal/model"
)

// KlineRepo implements kline.Repository backed by TiDB.
type KlineRepo struct {
	db *sql.DB
	tm *TableManager
}

func NewKlineRepo(db *sql.DB, tm *TableManager) (*KlineRepo, error) {
	return &KlineRepo{db: db, tm: tm}, nil
}

// BatchSave persists klines using REPLACE (upsert) semantics.
func (r *KlineRepo) BatchSave(ctx context.Context, symbol string, klines []*model.Kline) error {
	if len(klines) == 0 {
		return nil
	}

	if err := r.tm.EnsureTable(ctx, symbol); err != nil {
		return err
	}

	tableName := TableName(symbol)
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`
		REPLACE INTO %s (iv,st,ct,o,h,l,c,v,q,n,bv,bq,wavg,created_at,updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, tableName))
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, k := range klines {
		k.ComputeAvg()
		_, err = stmt.ExecContext(ctx,
			k.Interval, k.StartTime, k.CloseTime,
			k.Open.String(), k.High.String(), k.Low.String(), k.Close.String(),
			k.Volume.String(), k.Amount.String(),
			k.TradeCount,
			k.BuyTakerVol.String(), k.BuyTakerAmt.String(),
			k.WeightedAvg.String(),
			k.CreatedAt, k.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}
	}

	return tx.Commit()
}

// LoadKline retrieves a single kline by interval + startTime (PK lookup).
func (r *KlineRepo) LoadKline(ctx context.Context, symbol, interval string, startTime int64) (*model.Kline, error) {
	tableName := TableName(symbol)

	var k model.Kline
	var openS, highS, lowS, closeS string
	var volS, amtS string
	var bvS, bqS, wavgS string

	err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT iv,st,ct,o,h,l,c,v,q,n,bv,bq,wavg
		FROM %s WHERE iv=? AND st=?`, tableName), interval, startTime).Scan(
		&k.Interval, &k.StartTime, &k.CloseTime,
		&openS, &highS, &lowS, &closeS,
		&volS, &amtS, &k.TradeCount,
		&bvS, &bqS, &wavgS,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return nil, nil
		}
		return nil, fmt.Errorf("load kline: %w", err)
	}

	k.Open = parseDec(openS)
	k.High = parseDec(highS)
	k.Low = parseDec(lowS)
	k.Close = parseDec(closeS)
	k.Volume = parseDec(volS)
	k.Amount = parseDec(amtS)
	k.BuyTakerVol = parseDec(bvS)
	k.BuyTakerAmt = parseDec(bqS)
	k.WeightedAvg = parseDec(wavgS)

	return &k, nil
}

// Query retrieves historical klines for a symbol + interval + time range.
func (r *KlineRepo) Query(ctx context.Context, symbol, interval string,
	startTime, endTime int64, limit int) ([]*model.Kline, error) {

	tableName := TableName(symbol)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT iv,st,ct,o,h,l,c,v,q,n,bv,bq,wavg
		FROM %s
		WHERE iv = ? AND st >= ? AND st < ?
		ORDER BY st ASC
		LIMIT ?`, tableName), interval, startTime, endTime, limit)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return []*model.Kline{}, nil
		}
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var out []*model.Kline
	for rows.Next() {
		k := &model.Kline{}
		var openS, highS, lowS, closeS string
		var volS, amtS string
		var bvS, bqS, wavgS string
		if err := rows.Scan(
			&k.Interval, &k.StartTime, &k.CloseTime,
			&openS, &highS, &lowS, &closeS,
			&volS, &amtS, &k.TradeCount,
			&bvS, &bqS, &wavgS,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		k.Open = parseDec(openS)
		k.High = parseDec(highS)
		k.Low = parseDec(lowS)
		k.Close = parseDec(closeS)
		k.Volume = parseDec(volS)
		k.Amount = parseDec(amtS)
		k.BuyTakerVol = parseDec(bvS)
		k.BuyTakerAmt = parseDec(bqS)
		k.WeightedAvg = parseDec(wavgS)
		out = append(out, k)
	}
	return out, rows.Err()
}

func parseDec(s string) decimal.D {
	d, _ := decimal.FromString(s)
	return d
}
