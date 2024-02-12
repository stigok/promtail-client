package promtail

import (
	"log"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/golang/snappy"
	"github.com/martensson/promtail-client/logproto"
)

type protoLogEntry struct {
	entry *logproto.Entry
}

type clientProto struct {
	config    *ClientConfig
	quit      chan struct{}
	entries   chan protoLogEntry
	waitGroup sync.WaitGroup
	client    httpClient
}

func NewClientProto(conf ClientConfig) (Client, error) {
	client := clientProto{
		config:  &conf,
		quit:    make(chan struct{}),
		entries: make(chan protoLogEntry, LOG_ENTRIES_CHAN_SIZE),
		client:  httpClient{},
	}

	client.waitGroup.Add(1)
	go client.run()

	return &client, nil
}

func (c *clientProto) Log(message string) {
	c.LogWithTime(message, time.Now())
}

func (c *clientProto) LogWithTime(message string, t time.Time) {
	ns := t.UnixNano()
	c.entries <- protoLogEntry{
		entry: &logproto.Entry{
			Timestamp: &timestamp.Timestamp{
				Seconds: ns / int64(time.Second),
				Nanos:   int32(ns % int64(time.Second)),
			},
			Line: message,
		},
	}
}

func (c *clientProto) Shutdown() {
	close(c.quit)
	c.waitGroup.Wait()
}

func (c *clientProto) run() {
	var batch []*logproto.Entry
	batchSize := 0
	maxWait := time.NewTimer(c.config.BatchWait)

	defer func() {
		if batchSize > 0 {
			c.send(batch)
		}

		c.waitGroup.Done()
	}()

	for {
		select {
		case <-c.quit:
			return
		case entry := <-c.entries:
			batch = append(batch, entry.entry)
			batchSize++
			if batchSize >= c.config.BatchEntriesNumber {
				c.send(batch)
				batch = []*logproto.Entry{}
				batchSize = 0
				maxWait.Reset(c.config.BatchWait)
			}
		case <-maxWait.C:
			if batchSize > 0 {
				c.send(batch)
				batch = []*logproto.Entry{}
				batchSize = 0
			}
			maxWait.Reset(c.config.BatchWait)
		}
	}
}

func (c *clientProto) send(entries []*logproto.Entry) {
	var streams []*logproto.Stream
	streams = append(streams, &logproto.Stream{
		Labels:  c.config.Labels,
		Entries: entries,
	})

	req := logproto.PushRequest{
		Streams: streams,
	}

	buf, err := proto.Marshal(&req)
	if err != nil {
		log.Printf("promtail.ClientProto: unable to marshal: %s\n", err)
		return
	}

	buf = snappy.Encode(nil, buf)

	resp, body, err := c.client.sendJsonReq("POST", c.config.PushURL, "application/x-protobuf", buf)
	if err != nil {
		log.Printf("promtail.ClientProto: unable to send an HTTP request: %s\n", err)
		return
	}

	if resp.StatusCode != 204 {
		log.Printf("promtail.ClientProto: Unexpected HTTP status code: %d, message: %s\n", resp.StatusCode, body)
		return
	}
}
