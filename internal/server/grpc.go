package server

import (
	"context"
	"errors"

	makalukvv1 "github.com/georgikolev17/MakaluKV/api/makalukv/v1"
	"github.com/georgikolev17/MakaluKV/internal/service"
	"github.com/georgikolev17/MakaluKV/internal/storage/statemachine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	service *service.Service
	makalukvv1.UnimplementedKeyValueServiceServer
}

func NewGRPCServer(service *service.Service) *GRPCServer {
	return &GRPCServer{
		service: service,
	}
}

func (s *GRPCServer) Get(ctx context.Context, req *makalukvv1.GetRequest) (*makalukvv1.GetResponse, error) {
	value, found, err := s.service.Get(req.GetKey())
	if err != nil {
		return nil, translateError(err)
	}
	return &makalukvv1.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (s *GRPCServer) Put(ctx context.Context, req *makalukvv1.PutRequest) (*makalukvv1.PutResponse, error) {
	err := s.service.Put(req.GetKey(), req.GetValue())
	if err != nil {
		return nil, translateError(err)
	}
	return &makalukvv1.PutResponse{}, nil
}

func (s *GRPCServer) Delete(ctx context.Context, req *makalukvv1.DeleteRequest) (*makalukvv1.DeleteResponse, error) {
	err := s.service.Delete(req.GetKey())
	if err != nil {
		return nil, translateError(err)
	}
	return &makalukvv1.DeleteResponse{}, nil
}

func (s *GRPCServer) CompareAndSwap(ctx context.Context, req *makalukvv1.CompareAndSwapRequest) (*makalukvv1.CompareAndSwapResponse, error) {
	result, err := s.service.CompareAndSwap(req.GetKey(), req.GetExpectedValue(), req.GetNewValue())
	if err != nil {
		return nil, translateError(err)
	}
	return &makalukvv1.CompareAndSwapResponse{
		Result: translateSwapResult(result),
	}, nil
}

func translateError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, service.ErrInvalidKeyLength),
		errors.Is(err, service.ErrInvalidValueLength):
		return status.Error(codes.InvalidArgument, err.Error())

	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")

	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")

	default:
		return status.Error(codes.Internal, "internal server error")
	}
}

func translateSwapResult(result statemachine.SwapResult) makalukvv1.CompareAndSwapResult {
	switch result {
	case statemachine.SwapResultSwapped:
		return makalukvv1.CompareAndSwapResult_COMPARE_AND_SWAP_RESULT_SWAPPED
	case statemachine.SwapResultKeyNotFound:
		return makalukvv1.CompareAndSwapResult_COMPARE_AND_SWAP_RESULT_KEY_NOT_FOUND
	case statemachine.SwapResultValueMismatch:
		return makalukvv1.CompareAndSwapResult_COMPARE_AND_SWAP_RESULT_VALUE_MISMATCH
	default:
		return makalukvv1.CompareAndSwapResult_COMPARE_AND_SWAP_RESULT_UNSPECIFIED
	}
}
