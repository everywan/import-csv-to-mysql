package mtools

import (
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql" // mysql
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// ApplicationOptions 应用配置
type ApplicationOptions struct {
	Database DatabaseOptions `mapstructure:"database"`
}

// DatabaseOptions 数据库 简单设置
type DatabaseOptions struct {
	Driver    string `yaml:"driver" mapstructure:"driver"`
	Dsn       string `yaml:"dsn" mapstructure:"dsn"`
	KeepAlive int    `yaml:"keep_alive" mapstructure:"keep_alive"`
	MaxIdles  int    `yaml:"max_idles" mapstructure:"max_idles"`
	MaxOpens  int    `yaml:"max_opens" mapstructure:"max_opens"`
}

// NewDatabase 创建新的数据库对象
func NewDatabase(opts DatabaseOptions) (*gorm.DB, error) {
	o, err := gorm.Open(opts.Driver, opts.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "database open failed")
	}

	if opts.MaxIdles > 0 {
		o.DB().SetMaxIdleConns(opts.MaxIdles)
	}
	if opts.MaxOpens > 0 {
		o.DB().SetMaxOpenConns(opts.MaxOpens)
	}
	if opts.KeepAlive > 0 {
	}

	return o, nil
}

// Load 加载配置
func (s *ApplicationOptions) Load() {
	err := viper.Unmarshal(s)
	if err != nil {
		fmt.Printf("failed to parse config file: %s", err)
	}
}
