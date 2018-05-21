package tokenswap

import "time"

// Order tokenswap order
type Order struct {
	ID            int64     `xorm:"pk autoincr"`
	TX            string    `xorm:"index notnull"`
	From          string    `xorm:"notnull index(from_to_value)"`
	To            string    `xorm:"notnull index(from_to_value)"`
	Value         string    `xorm:"notnull index(from_to_value)"`
	InTx          string    `xorm:"index"`
	OutTx         string    `xorm:"index"`
	SendValue     string    `xorm:"notnull"`
	TaxCost       string    `xorm:"notnull"`
	CreateTime    time.Time `xorm:"TIMESTAMP notnull"`
	CompletedTime time.Time `xorm:"TIMESTAMP"`
}

// Log tokenswap order log
type Log struct {
	TX         string    `xorm:"index notnull"`
	CreateTime time.Time `xorm:"TIMESTAMP notnull"`
	Content    string    `xorm:"TEXT"`
}

type SendOrder struct {
	ID         int64     `xorm:"pk autoincr"`
	OrderTx    string    `xorm:"order_tx index notnull"`
	Status     int64     `xorm:"status"`
	OutTx      string    `xorm:"out_tx  index notnull"`
	To         string    `xorm:"notnull index(from_to_value)"`
	Value      string    `xorm:"notnull index(from_to_value)"`
	ToType     int32     `xorm:"to_type"` // 1 :ETH 2:NEO
	CreateTime time.Time `xorm:"TIMESTAMP notnull"`
}
