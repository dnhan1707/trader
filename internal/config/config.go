package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	MassiveKey  string
	MassiveBase string
	RedisAddr   string
	RedisPass   string
	RedisDB     int
	Port        string
	CacheTTL    int
}

func Load() *Config {
	_ = godotenv.Load()

	db, _ := strconv.Atoi(getenv("REDIS_DB", "0"))
	ttl, _ := strconv.Atoi(getenv("CACHE_TTL_SECONDS", "30"))

	c := &Config{
		MassiveKey:  getenv("MASSIVE_API_KEY", ""),
		MassiveBase: getenv("MASSIVE_BASE", "https://api.massive.com/v1"),
		RedisAddr:   getenv("REDIS_ADDR", "localhost:6379"),
		RedisPass:   getenv("REDIS_PASSWORD", ""),
		RedisDB:     db,
		Port:        getenv("PORT", "8080"),
		CacheTTL:    ttl,
	}

	if c.MassiveKey == "" {
		log.Println("WARNING: MASSIVE_API_KEY not set")
	}
	return c
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
