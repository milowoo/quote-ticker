package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"quote-ticker/internal/api"
	"quote-ticker/internal/config"
	"quote-ticker/internal/elector"
	"quote-ticker/internal/kafka"
	"quote-ticker/internal/kline"
	"quote-ticker/internal/metrics"
	"quote-ticker/internal/model"
	"quote-ticker/internal/repository"
	"quote-ticker/internal/ws"
)

func main() {
	// --- Config ---
	cfgPath := "config.yaml"
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		cfgPath = p
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Printf("using default config (load %s failed: %v)", cfgPath, err)
		cfg = config.Default()
	}

	// --- Database (read/write pool separation) ---
	// Write pool: used by checkpoint + completed kline writes (low concurrency).
	writeDB, err := sql.Open("mysql", cfg.Database.DSN)
	if err != nil {
		log.Fatalf("open write db: %v", err)
	}
	writeDB.SetMaxOpenConns(10)
	writeDB.SetMaxIdleConns(3)
	writeDB.SetConnMaxLifetime(5 * time.Minute)

	// Read pool: used by HTTP queries + recovery LoadKline (high concurrency).
	readDB, err := sql.Open("mysql", cfg.Database.DSN)
	if err != nil {
		log.Fatalf("open read db: %v", err)
	}
	readDB.SetMaxOpenConns(40)
	readDB.SetMaxIdleConns(10)
	readDB.SetConnMaxLifetime(5 * time.Minute)

	if err := writeDB.Ping(); err != nil {
		log.Fatalf("write db ping: %v", err)
	}
	if err := readDB.Ping(); err != nil {
		log.Fatalf("read db ping: %v", err)
	}
	log.Println("database connected (write=10, read=40)")

	// --- Context (used by checkpoint + Kafka + HTTP + elector) ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh
		log.Printf("received signal %v, shutting down...", sig)
		cancel()
	}()

	// --- Repository ---
	tm := repository.NewTableManager(writeDB)
	intervalTTL := map[string]time.Duration{
		"1m": 2 * time.Second,
	}
	repo, err := repository.NewKlineRepo(writeDB, readDB, tm, 500, intervalTTL, 5*time.Second)
	if err != nil {
		log.Fatalf("new repo: %v", err)
	}

	// --- Kline Aggregator ---
	agg := kline.NewAggregator(repo)

	// --- WebSocket Hub ---
	hub := ws.NewHub()

	// --- Leader Election ---
	var elect *elector.Elector
	leaderFn := func() bool { return true } // default: always leader (single instance)

	if cfg.Etcd.Enabled {
		log.Printf("connecting to etcd: %v", cfg.Etcd.Servers)

		instanceID, _ := os.Hostname()
		if instanceID == "" {
			instanceID = "unknown"
		}

		elect, err = elector.New(
			cfg.Etcd.Servers,
			cfg.Etcd.Path,
			instanceID,
			func(ctx context.Context) {
				log.Println("[leader] start leading — DB writes enabled")
				contInterval, _ := time.ParseDuration(cfg.Continuity.CheckInterval)
				if contInterval <= 0 {
					contInterval = 10 * time.Minute
				}
				go kline.NewContinuityChecker(repo, agg.DrainDirty, elect.IsLeader).Run(ctx, contInterval)
			},
			func() {
				log.Println("[leader] stop leading — DB writes disabled")
			},
		)
		if err != nil {
			log.Fatalf("elector: %v", err)
		}

		leaderFn = elect.IsLeader
		agg.SetLeaderChecker(leaderFn)

		go func() {
			for {
				v := float64(0)
				if elect.IsLeader() {
					v = 1
				}
				metrics.Leader.Set(v)
				time.Sleep(5 * time.Second)
			}
		}()

		go elect.Run(ctx)
		log.Println("leader election started, waiting for leadership...")
	} else {
		metrics.Leader.Set(1)
		log.Println("leader election disabled — single-instance mode")
	}

	// --- Start checkpoint only if leader check passes.
	agg.StartCheckpoint(ctx, 500*time.Millisecond)
	agg.StartStaleChecker(ctx, kline.StaleAlertConfig{
		Enabled:    true,
		Threshold:  2 * time.Minute,
		CheckEvery: 30 * time.Second,
	})

	// --- Trade handler (dispatches to aggregator + WebSocket) ---
	tradeHandler := func(ctx context.Context, trade model.Trade) error {
		if err := agg.ProcessTrade(ctx, trade); err != nil {
			return err
		}
		hub.BroadcastTrade(trade)
		return nil
	}

	// --- Kafka Consumer ---
	consumer := kafka.NewConsumer(cfg.Kafka, tradeHandler)

	// --- HTTP Server ---
	handler := api.NewHandler(repo)
	router := api.NewRouter(handler, hub)

	httpServer := &http.Server{
		Addr:         ":" + intStr(cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	// Start Kafka consumer
	go func() {
		log.Println("starting kafka consumer...")
		if err := consumer.Run(ctx); err != nil {
			log.Printf("kafka consumer exited: %v", err)
		}
	}()

	// Start HTTP server
	go func() {
		log.Printf("http server listening on :%d", cfg.Server.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Wait for shutdown
	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	consumer.Close()
	agg.Close(shutdownCtx)
	httpServer.Shutdown(shutdownCtx)
	if elect != nil {
		elect.Close()
	}
	writeDB.Close()
	readDB.Close()

	log.Println("shutdown complete")
}

func intStr(n int) string {
	if n == 0 {
		return "8082"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
