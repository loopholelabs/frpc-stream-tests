package stream_tests

import (
	"context"
	"io"
	"log"
)

type svc struct{}

func (s *svc) ExchangeNumbers(srv *ExchangeNumbersServer) error {
	//var max int32
	ctx := srv.Context()

	for {
		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// receive data from stream
		req, err := srv.Recv()
		if err == io.EOF {
			// return will close stream from server side
			res := Count{Result: 1000}
			err := srv.CloseAndSend(&res)
			log.Println(err)
			return nil
		}
		if err != nil {
			log.Println(err)
			continue
		}

		count := req.Result

		// Trigger interrupt test
		if count > 999 {
			err := srv.CloseSend()
			if err != nil {
				log.Println(err)
			}
		}

		resp := Count{Result: count + 1}
		if err := srv.Send(&resp); err != nil {
			log.Printf("send error %v", err)
		}
	}
}

func (s *svc) GetNumber(ctx context.Context, req *Request) (*Response, error) {
	res := Response{Count: req.InitialCount + 1}
	return &res, nil
}

func (s *svc) GetNumbers(req *Request, srv *GetNumbersServer) error {
	initial := req.InitialCount

	if initial > 999 {
		err := srv.CloseSend()
		if err != nil {
			log.Println(err)
		}
	}
	for i := initial; i < initial+99; i++ {
		res := Count{Result: i}
		err := srv.Send(&res)
		if err != nil {
			log.Printf("send error %v", err)
		}
	}
	return srv.CloseAndSend(&Count{Result: initial + 99})
}

func (s *svc) SendNumbers(srv *SendNumbersServer) error {
	var count int32 = 0
	for {
		res, err := srv.Recv()
		if err == io.EOF {
			// return will close stream from server side
			res := Response{Count: count + 1}
			err := srv.CloseAndSend(&res)
			if err != nil {
				log.Println(err)
			}
			return nil
		}
		if err != nil {
			log.Printf("receive error %v", err)
			continue
		}
		count = res.Result
		if count > 999 {
			return err
		}
	}
}
