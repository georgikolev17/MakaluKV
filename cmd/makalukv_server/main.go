package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	makalukvv1 "github.com/georgikolev17/MakaluKV/api/makalukv/v1"
	"github.com/georgikolev17/MakaluKV/internal/server"
	"github.com/georgikolev17/MakaluKV/internal/service"
	"github.com/georgikolev17/MakaluKV/internal/storage/statemachine"
	"google.golang.org/grpc"
)

func main() {
	listenAddress := flag.String(
		"listen-address",
		":50051",
		"TCP address on which the gRPC server listens",
	)

	dataDir := flag.String(
		"data-dir",
		"./data",
		"directory containing the Pebble database",
	)

	flag.Parse()

	if err := run(*listenAddress, *dataDir); err != nil {
		log.Fatal(err)
	}
}

func run(listenAddress string, dataDir string) error {
	pebbleStore, err := statemachine.OpenPebbleStore(dataDir)
	if err != nil {
		return fmt.Errorf("open Pebble store: %w", err)
	}

	defer func() {
		if err := pebbleStore.Close(); err != nil {
			log.Printf("close Pebble store: %v", err)
		}
	}()

	appService := service.NewService(pebbleStore)
	grpcAdapter := server.NewGRPCServer(appService)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("listen on port %s: %w", listenAddress, err)
	}

	grpcServer := grpc.NewServer()

	makalukvv1.RegisterKeyValueServiceServer(
		grpcServer,
		grpcAdapter,
	)

	shutdownSignal, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		<-shutdownSignal.Done()
		log.Println("shutting down gRPC server")
		grpcServer.GracefulStop()
	}()

	log.Println("gRPC server listening on", listenAddress)

	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}

	return nil
}
