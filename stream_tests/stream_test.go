package stream_tests

import (
	"context"
	"github.com/loopholelabs/testing/conn/pair"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestStreaming(t *testing.T) {
	cConn, sConn, err := pair.New()
	assert.NoErrorf(t, err, "Client server pair creation failed")

	client, err := NewClient(nil, nil)
	assert.NoErrorf(t, err, "Client creation failed")

	err = client.FromConn(cConn)
	assert.NoErrorf(t, err, "Client connection assignment failed")

	server, err := NewServer(new(svc), nil, nil)
	assert.NoErrorf(t, err, "Server creation failed")

	go server.ServeConn(sConn)

	t.Run("Bi-directional Streaming", func(t *testing.T) {
		t.Parallel()
		testBidirectional(client, t)
	})
	t.Run("Server Streaming", func(t *testing.T) {
		t.Parallel()
		testServerStreaming(client, t)
	})
	t.Run("Client Streaming", func(t *testing.T) {
		t.Parallel()
		testClientStreaming(client, t)
	})
	t.Run("Bidirectional Streaming Interrupted", func(t *testing.T) {
		t.Parallel()
		testInterruptedBidirectionalStream(client, t)
	})
	t.Run("Server Streaming Interrupted", func(t *testing.T) {
		t.Parallel()
		testInterruptedServerStream(client, t)
	})

	t.Run("Client Streaming Interrupted", func(t *testing.T) {
		t.Parallel()
		testClientStreamInterrupted(client, t)
	})
}

func testBidirectional(client *Client, t *testing.T) {
	initialCount := Count{Result: 0}
	ctx := context.Background()
	stream, err := client.ExchangeNumbers(ctx, &initialCount)
	assert.NoErrorf(t, err, "Starting bidirectional stream resulted in error")

	var lastResponse int32 = 0
	receivedFirstResponse := false

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			return
		}
		assert.NoErrorf(t, err, "Receiving from server in bidirectional streaming resulted in error")
		count := res.Result

		if receivedFirstResponse {
			assert.Equal(t, lastResponse+2, count)
			lastResponse = count
		} else {
			assert.Equal(t, int32(1), count)
			receivedFirstResponse = true
			lastResponse = count
		}
		if count == 99 {
			err := stream.CloseSend()
			assert.NoErrorf(t, err, "Closing send from client in bidirectional streaming resulted in error")
			return
		}

		count += 1
		req := Count{Result: count}
		err = stream.Send(&req)
		assert.NoErrorf(t, err, "Sending bidirectional response from client resulted in error")
	}
}

func testServerStreaming(client *Client, t *testing.T) {
	ctx := context.Background()
	var initialCount int32 = 5
	req := Request{InitialCount: initialCount}
	stream, err := client.GetNumbers(ctx, &req)
	assert.NoErrorf(t, err, "Opening server stream resulted in error")

	receivedFirstResponse := false
	var lastResponse int32

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			assert.Equal(t, initialCount+99, lastResponse, "Server stream did not finish on the correct value")
			return
		}
		assert.NoErrorf(t, err, "Receiving server streaming message resulted in error")
		count := res.Result
		if receivedFirstResponse == false {
			assert.Equal(t, initialCount, count)
			receivedFirstResponse = true
			lastResponse = count
		} else {
			assert.Equal(t, lastResponse+1, count)
			lastResponse += 1
		}
	}
}

func testClientStreaming(client *Client, t *testing.T) {
	ctx := context.Background()
	req := Count{Result: 0}
	stream, err := client.SendNumbers(ctx, &req)
	assert.NoErrorf(t, err, "Starting client stream resulted in error")

	for i := 1; i < 100; i++ {
		req := Count{Result: int32(i)}
		err := stream.Send(&req)
		assert.NoErrorf(t, err, "Sending client stream message resulted in error")
	}
	res, err := stream.CloseAndRecv()
	assert.NoErrorf(t, err, "Closing send stream resulted in error")
	assert.Equal(t, int32(100), res.Count)
}

func testInterruptedBidirectionalStream(client *Client, t *testing.T) {
	initialCount := Count{Result: 1000}
	ctx := context.Background()
	stream, err := client.ExchangeNumbers(ctx, &initialCount)
	assert.NoErrorf(t, err, "Starting bidirectional stream resulted in error")

	for {
		_, err := stream.Recv()
		assert.NotNilf(t, err, "Expected error object")
		assert.ErrorIs(t, err, io.EOF, "Expected EOF response")
		return
	}
}

func testInterruptedServerStream(client *Client, t *testing.T) {
	ctx := context.Background()
	var initialCount int32 = 1000
	req := Request{InitialCount: initialCount}
	stream, err := client.GetNumbers(ctx, &req)
	assert.NoErrorf(t, err, "Opening server stream resulted in error")

	for {
		_, err := stream.Recv()
		assert.NotNilf(t, err, "Expected error object")
		assert.ErrorIs(t, err, io.EOF, "Expected EOF response")
		return
	}
}

func testClientStreamInterrupted(client *Client, t *testing.T) {
	ctx := context.Background()
	req := Count{Result: 1000}
	stream, err := client.SendNumbers(ctx, &req)
	assert.NoErrorf(t, err, "Starting client stream resulted in error")

	for i := 1; i < 100; i++ {
		req := Count{Result: int32(i)}
		err := stream.Send(&req)
		assert.NoErrorf(t, err, "Sending client stream message resulted in error")
	}
	_, err = stream.CloseAndRecv()
	assert.NotNilf(t, err, "Expected error object")
	assert.ErrorIs(t, err, io.EOF, "Expected EOF response")
}
