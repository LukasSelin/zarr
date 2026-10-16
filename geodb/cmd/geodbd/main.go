// Command geodbd serves a geodb store over HTTP as JSON; see
// geodb.NewHandler for the routes.
//
//	geodbd -dir /data/geo.zarr -addr :8080 -cache-mb 1024
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
	addr := flag.String("addr", ":8080", "where to listen")
	cacheMB := flag.Int64("cache-mb", 512, "the most the chunk cache holds, in MiB")
	refresh := flag.Duration("refresh", 30*time.Second, "how often to look for what other processes wrote; 0 never")
	flag.Parse()
	if err := run(*dir, *addr, *cacheMB, *refresh); err != nil {
		log.Fatal(err)
	}
}

func run(dir, addr string, cacheMB int64, refresh time.Duration) error {
	if dir == "" {
		return errors.New("give the store with -dir")
	}
	// A store over HTTP is not here yet: zarr.NewHTTPStore is newer than the
	// tag of the core this module requires.
	var store zarr.Store = zarr.NewDirStore(dir)
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
	// On a signal, stop taking requests and let those under way finish, for
	// ten seconds at most, before returning.
	drained := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		drained <- srv.Shutdown(shut)
	}()
	log.Printf("serving %d tiles, %d weather sources and %d parcels on %s",
		len(db.Tiles()), len(db.WeatherSources()), db.NumParcels(), addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return <-drained
}
