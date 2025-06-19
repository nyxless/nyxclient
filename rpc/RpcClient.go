package rpc

import (
	"context"
	"fmt"
	"github.com/nyxless/nyx/x/pb"
	"google.golang.org/grpc"
)

type RpcClient struct {
	Address string
	client  pb.NYXRpcClient
	conn    *grpc.ClientConn
	rpcFail bool
}

func (c *RpcClient) Close() error {
	return c.conn.Close()
}

func (c *RpcClient) Request(ctx context.Context, method string, params map[string]any, opts []grpc.CallOption) (*pb.Reply, error) { //{{{
	var i int32
	keys := map[int32]string{}
	types := map[int32]string{}
	values := map[int32][]byte{}

	for k, v := range params {
		keys[i] = k
		if v != nil {
			switch val := v.(type) {
			case []byte:
				types[i] = "BYTES"
				values[i] = val
			case string:
				values[i] = []byte(val)
			default:
				values[i] = []byte(fmt.Sprint(val))
			}
		}

		i++
	}

	reply, err := c.client.Call(ctx, &pb.Request{Method: method, Keys: keys, Types: types, Values: values}, opts...)

	if err != nil {
		c.rpcFail = true
		return nil, fmt.Errorf("grpc call error: %+v", err)
	}

	return reply, nil
} // }}}
