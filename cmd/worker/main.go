package main

import (
	"log/slog"
	"os"

	"github.com/disillusioned-labs/ocr-gateway/internal/app"
	"github.com/disillusioned-labs/ocr-gateway/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "error", err)
		os.Exit(1)
	}
	if err := app.RunWorker(cfg); err != nil {
		slog.Error("shutdown with error", "error", err)
		os.Exit(1)
	}
}
