// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is copied from
// https://github.com/googleapis/google-cloud-go/blob/main/bigtable/internal/mockserver/inmem.go
// with minor modification.
package tests

import (
	"context"
	"net"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is an in-memory Cloud Bigtable fake.
// It is unauthenticated, and only a rough approximation.
type Server struct {
	Addr string

	l   net.Listener
	srv *grpc.Server

	// Any unimplemented methods will cause a panic when called.
	btpb.BigtableServer

	// Assign new functions to these parameters to implement specific mock
	// functionality.

	// ReadRowsFn mocks ReadRows.
	ReadRowsFn func(*btpb.ReadRowsRequest, btpb.Bigtable_ReadRowsServer) error
	// SampleRowKeysFn mocks SampleRowKeys.
	SampleRowKeysFn func(*btpb.SampleRowKeysRequest, btpb.Bigtable_SampleRowKeysServer) error
	// MutateRowFn mocks MutateRow.
	MutateRowFn func(context.Context, *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error)
	// MutateRowsFn mocks MutateRows.
	MutateRowsFn func(*btpb.MutateRowsRequest, btpb.Bigtable_MutateRowsServer) error
	// CheckAndMutateRowFn mocks CheckAndMutateRow.
	CheckAndMutateRowFn func(context.Context, *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error)
	// ReadModifyWriteRowFn mocks ReadModifyWriteRow.
	ReadModifyWriteRowFn func(context.Context, *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error)
	// ExecuteQueryFn mocks ExecuteQuery
	ExecuteQueryFn func(*btpb.ExecuteQueryRequest, btpb.Bigtable_ExecuteQueryServer) error
	// PrepareQueryFn mocks PrepareQuery
	PrepareQueryFn func(context.Context, *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error)
}

// NewServer creates a new Server.
// The Server will be listening for gRPC connections, without TLS,
// on the provided address. The resolved address is named by the Addr field.
func NewServer(laddr string, opt ...grpc.ServerOption) (*Server, error) {
	l, err := net.Listen("tcp", laddr)
	if err != nil {
		return nil, err
	}

	srv := grpc.NewServer(opt...)
	s := &Server{
		Addr: l.Addr().String(),
		l:    l,
		srv:  srv,
	}

	return s, nil
}

// Start starts the server
func (s *Server) Start() {
	btpb.RegisterBigtableServer(s.srv, s)
	go s.srv.Serve(s.l)
}

// Close closes the server.
func (s *Server) Close() error {
	if err := s.l.Close(); err != nil {
		return err
	}
	s.srv.Stop()
	return nil
}

// ReadRows implements ReadRows of the BigtableServer interface.
func (s *Server) ReadRows(req *btpb.ReadRowsRequest, srv btpb.Bigtable_ReadRowsServer) error {
	if s.ReadRowsFn != nil {
		return s.ReadRowsFn(req, srv)
	}
	return status.Error(codes.Unimplemented, "unimplemented - you need to attach a ReadRowsFn to the server")
}

// SampleRowKeys implements SampleRowKeys of the BigtableServer interface.
func (s *Server) SampleRowKeys(req *btpb.SampleRowKeysRequest, srv btpb.Bigtable_SampleRowKeysServer) error {
	if s.SampleRowKeysFn != nil {
		return s.SampleRowKeysFn(req, srv)
	}
	return status.Error(codes.Unimplemented, "unimplemented - you need to attach a SampleRowKeysFn to the server")
}

// MutateRow implements MutateRow of the BigtableServer interface.
func (s *Server) MutateRow(ctx context.Context, req *btpb.MutateRowRequest) (*btpb.MutateRowResponse, error) {
	if s.MutateRowFn != nil {
		return s.MutateRowFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented - you need to attach a MutateRowFn to the server")
}

// MutateRows implements MutateRows of the BigtableServer interface.
func (s *Server) MutateRows(req *btpb.MutateRowsRequest, srv btpb.Bigtable_MutateRowsServer) error {
	if s.MutateRowsFn != nil {
		return s.MutateRowsFn(req, srv)
	}
	return status.Error(codes.Unimplemented, "unimplemented - you need to attach a MutateRowsFn to the server")
}

// CheckAndMutateRow implements CheckAndMutateRow of the BigtableServer interface.
func (s *Server) CheckAndMutateRow(ctx context.Context, srv *btpb.CheckAndMutateRowRequest) (*btpb.CheckAndMutateRowResponse, error) {
	if s.CheckAndMutateRowFn != nil {
		return s.CheckAndMutateRowFn(ctx, srv)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented - you need to attach a CheckAndMutateRowFn to the server")
}

// ReadModifyWriteRow implements ReadModifyWriteRow of the BigtableServer interface.
func (s *Server) ReadModifyWriteRow(ctx context.Context, srv *btpb.ReadModifyWriteRowRequest) (*btpb.ReadModifyWriteRowResponse, error) {
	if s.ReadModifyWriteRowFn != nil {
		return s.ReadModifyWriteRowFn(ctx, srv)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented - you need to attach a ReadModifyWriteRowFn to the server")
}

func (s *Server) ExecuteQuery(req *btpb.ExecuteQueryRequest, srv btpb.Bigtable_ExecuteQueryServer) error {
	if s.ExecuteQueryFn != nil {
		return s.ExecuteQueryFn(req, srv)
	}
	return status.Error(codes.Unimplemented, "unimplemented - you need to attach a ExecuteQueryFn to the server")
}

func (s *Server) PrepareQuery(ctx context.Context, req *btpb.PrepareQueryRequest) (*btpb.PrepareQueryResponse, error) {
	if s.PrepareQueryFn != nil {
		return s.PrepareQueryFn(ctx, req)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented - you need to attach a PrepareQueryFn to the server")
}
