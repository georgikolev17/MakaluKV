package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	makalukvv1 "github.com/georgikolev17/MakaluKV/api/makalukv/v1"
	"github.com/georgikolev17/MakaluKV/internal/server"
	"github.com/georgikolev17/MakaluKV/internal/service"
	"github.com/georgikolev17/MakaluKV/internal/storage"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	pebbleStore, err := storage.OpenPebbleStore("./data")
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

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		return fmt.Errorf("listen on port 50051: %w", err)
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

	log.Println("gRPC server listening on :50051")

	if err := grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}

	return nil
}
