package integration

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	makalukvv1 "github.com/georgikolev17/MakaluKV/api/makalukv/v1"
	"github.com/georgikolev17/MakaluKV/internal/server"
	"github.com/georgikolev17/MakaluKV/internal/service"
	"github.com/georgikolev17/MakaluKV/internal/storage/statemachine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufferSize = 1024 * 1024

func TestGRPCKeyValueLifecycle(t *testing.T) {
	// Use an isolated temporary Pebble database.
	pebbleStore, err := statemachine.OpenPebbleStore(t.TempDir())
	if err != nil {
		t.Fatalf("open Pebble store: %v", err)
	}

	t.Cleanup(func() {
		if err := pebbleStore.Close(); err != nil {
			t.Errorf("close Pebble store: %v", err)
		}
	})

	appService := service.NewService(pebbleStore)
	grpcAdapter := server.NewGRPCServer(appService)

	// Start a real gRPC server over an in-memory connection.
	listener := bufconn.Listen(bufferSize)
	grpcServer := grpc.NewServer()

	makalukvv1.RegisterKeyValueServiceServer(
		grpcServer,
		grpcAdapter,
	)

	serveErr := make(chan error, 1)

	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()

		if err := <-serveErr; err != nil {
			t.Errorf("serve gRPC: %v", err)
		}
	})

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gRPC client connection: %v", err)
	}

	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("close gRPC client connection: %v", err)
		}
	})

	client := makalukvv1.NewKeyValueServiceClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := []byte("hello")
	initialValue := []byte("world")
	replacementValue := []byte("universe")

	// Put: hello -> world.
	if _, err := client.Put(ctx, &makalukvv1.PutRequest{
		Key:   key,
		Value: initialValue,
	}); err != nil {
		t.Fatalf("Put returned an error: %v", err)
	}

	// Get: confirm hello -> world.
	getResponse, err := client.Get(ctx, &makalukvv1.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("first Get returned an error: %v", err)
	}

	if !getResponse.GetFound() {
		t.Fatal("first Get returned found=false")
	}

	if !bytes.Equal(getResponse.GetValue(), initialValue) {
		t.Fatalf(
			"first Get returned value %q; expected %q",
			getResponse.GetValue(),
			initialValue,
		)
	}

	// CAS: world -> universe.
	casResponse, err := client.CompareAndSwap(
		ctx,
		&makalukvv1.CompareAndSwapRequest{
			Key:           key,
			ExpectedValue: initialValue,
			NewValue:      replacementValue,
		},
	)
	if err != nil {
		t.Fatalf("CompareAndSwap returned an error: %v", err)
	}

	if casResponse.GetResult() !=
		makalukvv1.CompareAndSwapResult_COMPARE_AND_SWAP_RESULT_SWAPPED {
		t.Fatalf(
			"CompareAndSwap returned %s; expected SWAPPED",
			casResponse.GetResult(),
		)
	}

	// Get: confirm CAS changed the value.
	getResponse, err = client.Get(ctx, &makalukvv1.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("second Get returned an error: %v", err)
	}

	if !getResponse.GetFound() {
		t.Fatal("second Get returned found=false")
	}

	if !bytes.Equal(getResponse.GetValue(), replacementValue) {
		t.Fatalf(
			"second Get returned value %q; expected %q",
			getResponse.GetValue(),
			replacementValue,
		)
	}

	// Delete the entry.
	if _, err := client.Delete(ctx, &makalukvv1.DeleteRequest{
		Key: key,
	}); err != nil {
		t.Fatalf("Delete returned an error: %v", err)
	}

	// Final Get: confirm the entry was actually deleted.
	getResponse, err = client.Get(ctx, &makalukvv1.GetRequest{
		Key: key,
	})
	if err != nil {
		t.Fatalf("final Get returned an error: %v", err)
	}

	if getResponse.GetFound() {
		t.Fatalf(
			"final Get returned found=true with value %q; expected the key to be deleted",
			getResponse.GetValue(),
		)
	}

	if len(getResponse.GetValue()) != 0 {
		t.Fatalf(
			"final Get returned value %q for a missing key; expected an empty value",
			getResponse.GetValue(),
		)
	}
}
