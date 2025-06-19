package rpc

import (
	"context"
	"google.golang.org/grpc"
	"time"
)

type RpcOptions struct {
	Timeout      time.Duration
	MaxIdleConns int
	MaxOpenConns int
	Headers      map[string]string
	Ctx          context.Context
	UseCache     bool
	CacheTtl     int
	Debug        bool
}

type DialOption struct {
	grpc.EmptyDialOption
	F func(*RpcOptions)
}

type CallOption struct {
	grpc.EmptyCallOption
	F func(*RpcOptions)
}
