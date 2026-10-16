// Command geodbd serves a geodb store over HTTP as JSON; see
// geodb.NewHandler for the routes.
//
//	geodbd -dir /data/geo.zarr -addr :8080 -cache-mb 1024
//	geodbd -url https://example.com/geo.zarr
//
// It reads the store through a cache, and takes a new snapshot of it every
// -refresh, so that scenes ingested by another process are served.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/LukasSelin/zarr"
	"github.com/LukasSelin/zarr/geodb"
)

func main() {
	dir := flag.String("dir", "", "the store, a directory")
	url := flag.String("url", "", "the store, published over HTTP (read only)")
	addr := flag.String("addr", ":8080", "where to listen")
	cacheMB := flag.Int64("cache-mb", 512, "the most the chunk cache holds, in MiB")
	refresh := flag.Duration("refresh", 30*time.Second, "how often to look for what other processes wrote; 0 never")
	flag.Parse()
	if err := run(*dir, *url, *addr, *cacheMB, *refresh); err != nil {
		log.Fatal(err)
	}
}

func run(dir, url, addr string, cacheMB int64, refresh time.Duration) error {
	var store zarr.Store
	switch {
	case (dir == "") == (url == ""):
		return errors.New("give one of -dir and -url")
	case dir != "":
		store = zarr.NewDirStore(dir)
	default:
		s, err := zarr.NewHTTPStore(url, nil)
		if err != nil {
			return err
		}
		store = s
	}
	cached := geodb.NewCachedStore(store, geodb.CacheOptions{MaxBytes: cacheMB << 20})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := geodb.Open(ctx, cached)
	if err != nil {
		return fmt.Errorf("opening the store: %w", err)
	}
	if refresh > 0 {
		go func() {
			tick := time.NewTicker(refresh)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if err := db.Refresh(ctx); err != nil && ctx.Err() == nil {
						log.Printf("refresh: %v", err)
					}
				}
			}
		}()
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           geodb.NewHandler(db),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	log.Printf("serving %d tiles, %d weather sources and %d parcels on %s",
		len(db.Tiles()), len(db.WeatherSources()), db.NumParcels(), addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
