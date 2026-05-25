package model

import "quote-ticker/internal/decimal"

// Kline represents one candlestick record.
// All price/volume fields use fixed-point int64 (10^8 precision, zero heap alloc).
type Kline struct {
	Interval   string `json:"i"`
	StartTime  int64  `json:"t"`
	CloseTime  int64  `json:"T"`

	Open   decimal.D `json:"o"`
	High   decimal.D `json:"h"`
	Low    decimal.D `json:"l"`
	Close  decimal.D `json:"c"`

	Volume      decimal.D `json:"v"`
	Amount      decimal.D `json:"q"`
	BuyTakerVol decimal.D `json:"V"`
	BuyTakerAmt decimal.D `json:"Q"`

	TradeCount int32 `json:"n"`

	// WeightedAvg is computed lazily at flush time (not per trade).
	WeightedAvg decimal.D `json:"w"`

	CreatedAt int64 `json:"-"`
	UpdatedAt int64 `json:"-"`
}

// NewKline creates an initial kline from the first trade of the period.
func NewKline(interval string, startTime, closeTime int64, price decimal.D) *Kline {
	now := timeNowMs()
	return &Kline{
		Interval:  interval,
		StartTime: startTime,
		CloseTime: closeTime,
		Open:      price,
		High:      price,
		Low:       price,
		Close:     price,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// Update applies a trade to this kline. Only OHLCV + trade count updates;
// WeightedAvg is computed later at flush time.
func (k *Kline) Update(price, quantity, amount decimal.D, takerBuy bool) {
	if price.Cmp(k.High) > 0 {
		k.High = price
	}
	if price.Cmp(k.Low) < 0 {
		k.Low = price
	}
	k.Close = price

	k.Volume = k.Volume.Add(quantity)
	k.Amount = k.Amount.Add(amount)
	k.TradeCount++

	if takerBuy {
		k.BuyTakerVol = k.BuyTakerVol.Add(quantity)
		k.BuyTakerAmt = k.BuyTakerAmt.Add(amount)
	}

	k.UpdatedAt = timeNowMs()
}

// ComputeAvg sets the weighted average price (amount / volume).
func (k *Kline) ComputeAvg() {
	if k.Volume.Sign() > 0 {
		k.WeightedAvg = k.Amount.Quo(k.Volume)
	}
}
