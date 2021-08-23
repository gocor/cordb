package cordb

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

// ClusterProviderConfig ...
type ClusterProviderConfig struct {
	DriverName string

	ConnectionString string
	DB               func() *sqlx.DB
	SetDB            func(db *sqlx.DB)
	Once             *sync.Once

	ReaderConnectionString string
	ReaderDB               func() *sqlx.DB
	SetReaderDB            func(db *sqlx.DB)
	ReaderOnce             *sync.Once
}

// ClusterProvider ...
type ClusterProvider interface {
	Open() (*sqlx.DB, error)
	OpenReader() (*sqlx.DB, error)
	OpenNode(useReader bool) (*sqlx.DB, error)
}

type clusterProvider struct {
	provider       Provider
	readerProvider Provider
}

// NewClusterProvider ...
func NewClusterProvider(cfg ClusterProviderConfig) ClusterProvider {
	cfgWriter := ProviderConfig{
		ConnectionString: cfg.ConnectionString,
		DB:               cfg.DB,
		SetDB:            cfg.SetDB,
		DriverName:       cfg.DriverName,
		Once:             cfg.Once,
	}
	cfgReader := ProviderConfig{
		ConnectionString: cfg.ReaderConnectionString,
		DB:               cfg.ReaderDB,
		SetDB:            cfg.SetReaderDB,
		DriverName:       cfg.DriverName,
		Once:             cfg.ReaderOnce,
	}

	return &clusterProvider{
		provider:       NewProvider(cfgWriter),
		readerProvider: NewProvider(cfgReader),
	}
}

// Open will open the writer
func (p *clusterProvider) Open() (*sqlx.DB, error) {
	return p.provider.Open()
}

// OpenReader will open the reader
func (p *clusterProvider) OpenReader() (*sqlx.DB, error) {
	return p.readerProvider.Open()
}

// OpenNode will give you a choice toopen the writer or reader
func (p *clusterProvider) OpenNode(useReader bool) (*sqlx.DB, error) {
	if useReader {
		return p.OpenReader()
	}
	return p.Open()
}
