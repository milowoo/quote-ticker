package model

import (
	"quote-ticker/internal/decimal"
	"quote-ticker/internal/model/pb"
)

// Trade represents a single executed trade tick from the matching engine.
type Trade struct {
	Symbol    string
	TradeID   int64
	Price     decimal.D
	Quantity  decimal.D
	Amount    decimal.D // computed: price * quantity
	TakerBuy  bool
	Timestamp int64 // epoch millis
}

// NewTradeFromTick converts a protobuf PbTradeTick into a domain Trade.
// Amount is computed as price × quantity.
func NewTradeFromTick(tick *pb.PbTradeTick) Trade {
	price, _ := decimal.FromString(tick.Price)
	qty, _ := decimal.FromString(tick.Quantity)

	t := Trade{
		Symbol:    tick.SymbolId,
		TradeID:   tick.TradeId,
		Price:     price,
		Quantity:  qty,
		TakerBuy:  tick.TakerSide == "BUY",
		Timestamp: tick.TradeTime,
	}
	t.Amount = price.Mul(qty)
	return t
}
