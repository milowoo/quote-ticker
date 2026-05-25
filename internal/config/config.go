package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Database   DatabaseConfig   `yaml:"database"`
	ZooKeeper  ZooKeeperConfig  `yaml:"zookeeper"`
	Continuity ContinuityConfig `yaml:"continuity"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type KafkaConfig struct {
	Brokers  []string `yaml:"brokers"`
	Topic    string   `yaml:"topic"`
	GroupID  string   `yaml:"group_id"`
	Parallel int      `yaml:"parallel"`
}

type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

type ZooKeeperConfig struct {
	Enabled bool     `yaml:"enabled"`
	Servers []string `yaml:"servers"`
	Path    string   `yaml:"path"`
}

type ContinuityConfig struct {
	CheckInterval string `yaml:"check_interval"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func Default() *Config {
	return &Config{
		Server: ServerConfig{Port: 8082},
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "match-result",
			GroupID:  "quote-ticker-group",
			Parallel: 1,
		},
		Database: DatabaseConfig{
			DSN: "root:@tcp(localhost:4000)/quote_ticker?charset=utf8mb4&parseTime=true&loc=UTC",
		},
		ZooKeeper: ZooKeeperConfig{
			Enabled: false,
			Servers: []string{"localhost:2181"},
			Path:    "/quote-ticker/leader",
		},
		Continuity: ContinuityConfig{
			CheckInterval: "10m",
		},
	}
}
