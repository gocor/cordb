package cordb

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

// ProviderConfig ...
type ProviderConfig struct {
	ConnectionString string
	DB               func() *sqlx.DB
	SetDB            func(db *sqlx.DB)
	DriverName       string
	Once             *sync.Once
}

// Provider ...
type Provider interface {
	Open() (*sqlx.DB, error)
}

type provider struct {
	cfg ProviderConfig
}

// NewProvider ...
func NewProvider(cfg ProviderConfig) Provider {
	return &provider{
		cfg: cfg,
	}
}

// Open ...
func (p *provider) Open() (*sqlx.DB, error) {
	if p.cfg.DB() == nil {
		var err error
		p.cfg.Once.Do(func() {
			var db *sqlx.DB
			db, err = sqlx.Connect(p.cfg.DriverName, p.cfg.ConnectionString)
			if err == nil {
				p.cfg.SetDB(db)
			}
		})
		if err != nil {
			return nil, err
		}
	}

	return p.cfg.DB(), nil
}
