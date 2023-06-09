package config

import (
	"github.com/kelseyhightower/envconfig"
)

// Config represents service configuration for dp-search-reindex-batch
type Config struct {
	SomeValue string `envconfig:"SOME_VALUE"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		SomeValue: "something",
	}

	return cfg, envconfig.Process("", cfg)
}
