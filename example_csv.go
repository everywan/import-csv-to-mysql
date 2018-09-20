package mtools

import "time"

// 导入后使用状态
const (
	ExampleCSVStatusUnBind = iota
	ExampleCSVStatusBind
)

type (
	// ExampleCSV is 导入 merchant 数据
	ExampleCSV struct {
		ID                int    `gorm:"column:id" json:"id"`
		BusinessAccountID int    `gorm:"column:business_account_id" json:"business_account_id"`
		MobilePhone       int64  `gorm:"column:mobile_phone" json:"mobile_phone"`
		Balance           int    `gorm:"column:balance" json:"balance"`
		ExpiredAt         string `gorm:"column:expired_at" json:"expired_at"`
		Status            int    `gorm:"column:status" json:"status"`

		CreatedAt time.Time  `gorm:"column:created_at"  json:"created_at"`
		UpdatedAt time.Time  `gorm:"column:updated_at"  json:"updated_at"`
		DeletedAt *time.Time `gorm:"column:deleted_at"  json:"deleted_at"`
	}
	// ExampleCSVService is
	ExampleCSVService interface {
	}
)

// TableName 数据库表名
func (ExampleCSV) TableName() string {
	return "merchant_svc_to_imports"
}
