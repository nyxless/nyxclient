package nyxclient

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/nyxless/nyx/x"
	"github.com/nyxless/nyx/x/cache"
	"github.com/nyxless/nyx/x/log"
	"github.com/nyxless/nyx/x/pb"
	"github.com/nyxless/nyxclient/rpc"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"strconv"
	"sync"
	"time"
)

type NyxClient struct {
	Address string
	appid   string
	secret  string
	rpc     *rpc.RpcPool
	logger  *log.Logger
	cache   *cache.Cache
}

var (
	//缓存开关，需要Request时使用WithCache同时保持UseCache开关开启
	DefaultUseCache = false

	//默认GRPC连接超时时间3秒
	DefaultDialTimeout = time.Duration(3) * time.Second

	//默认请求超时时间3秒
	DefaultTimeout = time.Duration(3) * time.Second

	//默认最大空闲连接数
	DefaultMaxIdleConns = 10

	//默认最大连接数
	DefaultMaxOpenConns = 2000
)

var nyxclient_instance = map[string]*NyxClient{}
var mutex sync.RWMutex
var group singleflight.Group

// 设置参数MaxIdleConns, 支持方法: NewNyxClient
func WithMaxIdleConns(mic int) grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.MaxIdleConns = mic
		},
	}
} // }}}

// 设置参数MaxOpenConns, 支持方法: NewNyxClient
func WithMaxOpenConns(moc int) grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.MaxOpenConns = moc
		},
	}
} // }}}

// 设置参数Debug, 支持方法:NewNyxClient
func Debug() grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.Debug = true
		},
	}
} // }}}

// 设置参数UseCache, 支持方法:NewNyxClient
func UseCache() grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.UseCache = true
		},
	}
} // }}}

// 设置参数 Timeout, 支持方法: Request, 表示请求超时时间 (若设置grpc连接超时时间, 在 NewNyxClient 中使用 grpc.WithTimeout)
func WithTimeout(timeout time.Duration) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Timeout = timeout
		},
	}
} // }}}

// 设置参数Ctx, 支持方法: Request
func WithContext(ctx context.Context) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Ctx = ctx
		},
	}
} // }}}

// 设置参数Headers, 支持方法: Request
func WithHeaders(headers map[string]string) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Headers = headers
		},
	}
} // }}}

// 设置参数UseCache、CacheTtl, 支持方法: Request
func WithCache(ttl int) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.UseCache = true
			r.CacheTtl = ttl
		},
	}
} // }}}

func NewNyxClient(address, appid, secret string, opts ...grpc.DialOption) (*NyxClient, error) { //{{{
	mutex.RLock()
	ins, ok := nyxclient_instance[address]
	mutex.RUnlock()

	if !ok {
		mutex.Lock()
		defer mutex.Unlock()

		var err error
		ins, err = newNyxClient(address, appid, secret, opts...)
		if nil != err {
			return nil, err
		}

		nyxclient_instance[address] = ins
	}

	return ins, nil
} // }}}

func newNyxClient(address, appid, secret string, opts ...grpc.DialOption) (*NyxClient, error) { //{{{
	options := &rpc.RpcOptions{
		MaxIdleConns: DefaultMaxIdleConns,
		MaxOpenConns: DefaultMaxOpenConns,
		UseCache:     DefaultUseCache,
		Debug:        false,
	}

	grpc_opts := []grpc.DialOption{
		grpc.WithTimeout(DefaultDialTimeout),
	}

	for _, opt := range opts {
		if o, ok := opt.(*rpc.DialOption); ok {
			o.F(options)
		} else {
			grpc_opts = append(grpc_opts, opt)
		}
	}

	if options.MaxIdleConns <= 0 {
		options.MaxIdleConns = 1
	}

	if options.MaxOpenConns <= 0 {
		options.MaxOpenConns = 1
	}

	if options.MaxIdleConns > options.MaxOpenConns {
		options.MaxIdleConns = options.MaxOpenConns
	}

	logger := x.Logger
	if logger == nil { //如果在nyx框架项目中，直接使用x.logger的配置
		var err error
		logger, err = getLogger()
		if err != nil {
			return nil, err
		}
	}

	rpclient, err := rpc.New(address, options.MaxIdleConns, options.MaxOpenConns, grpc_opts)
	if err != nil {
		logger.Log("nyxclient", err)
		return nil, err
	}

	if options.Debug {
		rpc.Debug = true
	}

	nyxclient := &NyxClient{
		Address: address,
		appid:   appid,
		secret:  secret,
		rpc:     rpclient,
		logger:  logger,
	}

	if options.UseCache {
		nyxclient.cache = x.NewLocalCache()
	}

	fmt.Println("nyxclient init:", address)

	return nyxclient, nil
} // }}}

func getLogger() (*log.Logger, error) { // {{{
	logger, err := log.NewLogger()
	if err != nil {
		return nil, err
	}

	logger.SetLevel(log.LevelCustom)

	fwriter, err := log.NewFileWriter("logs", "nyxclient-2006-01-02.log")
	if err != nil {
		return nil, err
	}

	fwriter_err, err := log.NewFileWriter("logs", "nyxclient-err-2006-01-02.log")
	if err != nil {
		return nil, err
	}

	logger.AddWriter(fwriter, "nyxclient")         //使用自定义日志名:nyxclient
	logger.AddWriter(fwriter_err, "nyxclient-err") //使用自定义日志名:nyxclient

	return logger, nil
} // }}}

func (this *NyxClient) Request(method string, params any, opts ...grpc.CallOption) (*Response, error) { // {{{
	var data map[string]any
	var err error

	if params != nil {
		data = x.AsMap(params)
	}

	options := &rpc.RpcOptions{
		Timeout: DefaultTimeout,
		Headers: map[string]string{},
	}

	grpc_opts := []grpc.CallOption{}
	for _, opt := range opts {
		if o, ok := opt.(*rpc.CallOption); ok {
			o.F(options)
		} else {
			grpc_opts = append(grpc_opts, opt)
		}
	}

	options.Headers["Appid"] = this.appid
	options.Headers["Nonce"] = x.AsString(x.RandUint32())
	options.Headers["Timestamp"] = x.AsString(x.Now())
	options.Headers["Authorization"] = "Bearer " + x.Sha256(options.Headers["Appid"]+options.Headers["Nonce"]+options.Headers["Timestamp"], this.secret)

	var ctx context.Context
	if options.Ctx != nil {
		ctx = options.Ctx

		if v := ctx.Value("guid"); v != nil {
			if guid, ok := v.(string); ok {
				options.Headers["guid"] = guid
			}
		}
	} else {
		ctx = context.Background()
	}

	if options.Headers["guid"] == "" {
		options.Headers["guid"] = x.GetUUID()
	}

	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}

	md := metadata.MD{}
	for k, v := range options.Headers {
		md.Append(k, v)
	}

	ctx = metadata.NewOutgoingContext(ctx, md)

	var response *Response
	var reply *pb.Reply
	var header, trailer metadata.MD
	var cache_key []byte
	var hit_cache bool
	var real bool

	start_time := time.Now()

	if options.UseCache && this.cache != nil {
		cache_key = this.getCacheKey(method, data)
		cache_data, found := this.getFromCache(cache_key)
		if !found {
			result, err, _ := group.Do(string(cache_key), func() (interface{}, error) {
				grpc_opts = append(grpc_opts, grpc.Header(&header), grpc.Trailer(&trailer))
				reply, err = this.rpc.Request(ctx, method, data, grpc_opts)
				if nil != err {
					this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, data)
					return nil, err
				}

				cache_data := &cacheData{reply, header, trailer}

				if reply.Code == 0 { // 返回成功时，更新缓存
					this.setCache(cache_key, cache_data, options.CacheTtl)
				}

				real = true

				return cache_data, nil
			})

			if nil != err {
				return nil, err
			}

			hit_cache = !real
			cache_data = result.(*cacheData)
		} else {
			hit_cache = true
		}

		reply = cache_data.Reply
		header = cache_data.Header
		trailer = cache_data.Trailer

	} else {
		grpc_opts = append(grpc_opts, grpc.Header(&header), grpc.Trailer(&trailer))
		reply, err = this.rpc.Request(ctx, method, data, grpc_opts)
		if nil != err {
			this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, data)
			return nil, err
		}
	}

	code := reply.Code
	msg := reply.Msg
	consume := reply.Consume
	server_time := reply.Time
	res, err := this.process(reply.Data)
	if err != nil {
		return nil, err
	}

	consume_t := int(time.Since(start_time).Milliseconds())

	this.logger.Log("nyxclient", map[string]any{"appid": this.appid, "uri": method, "cache": hit_cache}, data, map[string]any{"code": code, "consume_t": consume_t, "consume": consume, "data": res, "msg": msg, "time": server_time})
	if code > 0 {
		this.logger.Log("nyxclient-err", map[string]any{"appid": this.appid, "uri": method}, data, map[string]any{"code": code, "consume_t": consume_t, "consume": consume, "data": res, "msg": msg, "time": server_time})
	}

	response = &Response{
		Code:    code,
		Msg:     msg,
		Data:    res,
		Header:  header,
		Trailer: trailer,
	}

	return response, nil
} //}}}

func (this *NyxClient) Close() {
	this.logger.Close()
}

func (this *NyxClient) process(r *pb.ReplyData) (map[string]any, error) { //{{{
	if r == nil || r.Keys == nil || r.Values == nil {
		return nil, nil
	}

	res := map[string]any{}
	for k, v := range r.Keys {
		t, _ := r.Types[k]
		switch t {
		case "BYTES":
			res[v] = r.Values[k]
		case "STRING":
			res[v] = string(r.Values[k])
		case "JSON":
			res[v] = x.JsonDecode(r.Values[k])
		default:
			res[v] = x.AsString(r.Values[k])
		}
	}

	return res, nil
} // }}}

func (this *NyxClient) getFromCache(key []byte) (*cacheData, bool) { // {{{
	got, err := this.cache.Get(key)

	if err != nil {
		return nil, false
	}

	var cache_data *cacheData
	decoder := gob.NewDecoder(bytes.NewReader(got))
	err = decoder.Decode(&cache_data)
	if err != nil {
		return nil, false
	}

	return cache_data, true
} // }}}

func (this *NyxClient) setCache(key []byte, val *cacheData, ttl int) error { // {{{
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	encodedData := buf.Bytes()

	return this.cache.Set(key, encodedData, ttl)
} // }}}

func (this *NyxClient) getCacheKey(method string, data map[string]any) []byte {
	return []byte(method + strconv.Itoa(int(x.HashMap(data))))
}

type cacheData struct {
	Reply   *pb.Reply
	Header  metadata.MD
	Trailer metadata.MD
}

type Response struct {
	Data    map[string]any
	Code    int32
	Msg     string
	Header  metadata.MD
	Trailer metadata.MD
}

// 获取返回结果的data对象
func (r *Response) GetData() map[string]any { // {{{
	return r.Data
} // }}}

// 获取返回结果的data对象中key对应的值
func (r *Response) GetValue(key string) any {
	return r.Data[key]
}

func (r *Response) GetCode() int32 {
	return r.Code
}

func (r *Response) GetMsg() string {
	return r.Msg
}

func (r *Response) GetHeaders() metadata.MD {
	return r.Header
}

func (r *Response) GetHeader(k string) string { // {{{
	h := r.Header.Get(k)
	if len(h) > 0 {
		return h[0]
	}

	return ""
} // }}}

func (r *Response) GetTrailers() metadata.MD {
	return r.Trailer
}

func (r *Response) GetTrailer(k string) string { // {{{
	t := r.Trailer.Get(k)
	if len(t) > 0 {
		return t[0]
	}

	return ""
} // }}}
