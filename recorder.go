package xdstest

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Recorder is a gRPC stream interceptor that records every stream to and from your server:
//
//	rec := &xdstest.Recorder{}
//	server := grpc.NewServer(grpc.StreamInterceptor(rec.Intercept))
//
// Use one Recorder for the whole test, so a reconnect shows up as a second conversation.
type Recorder struct {
	mu            sync.Mutex
	conversations []*Conversation
}

// Intercept is a [grpc.StreamServerInterceptor].
func (r *Recorder) Intercept(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	c := &Conversation{last: time.Now()}
	r.mu.Lock()
	r.conversations = append(r.conversations, c)
	r.mu.Unlock()
	err := handler(srv, &recordingStream{ServerStream: ss, c: c})
	c.finish()
	return err
}

// Conversations returns the streams recorded so far, oldest first.
func (r *Recorder) Conversations() []*Conversation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*Conversation(nil), r.conversations...)
}

// Conversation is one recorded stream: requests from Envoy and responses to it, in order.
type Conversation struct {
	mu       sync.Mutex
	messages []proto.Message
	last     time.Time
	finished bool
}

// NewConversation returns an empty conversation, e.g. to test your own checks against.
func NewConversation() *Conversation {
	return &Conversation{last: time.Now()}
}

// Add records a message.
func (c *Conversation) Add(m proto.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, proto.Clone(m))
	c.last = time.Now()
}

// Messages returns a copy of the messages so far.
func (c *Conversation) Messages() []proto.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]proto.Message(nil), c.messages...)
}

// Size is the number of messages so far. Take it as a mark before a change, so later checks only look at what came
// after.
func (c *Conversation) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

// QuietFor reports whether nothing has been sent either way for at least d.
func (c *Conversation) QuietFor(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.last) >= d
}

// Finished reports whether the stream has ended.
func (c *Conversation) Finished() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finished
}

func (c *Conversation) finish() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finished = true
}

type recordingStream struct {
	grpc.ServerStream
	c *Conversation
}

func (s *recordingStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if pm, ok := m.(proto.Message); ok && err == nil {
		s.c.Add(pm)
	}
	return err
}

func (s *recordingStream) SendMsg(m any) error {
	// Record before sending, so the response is always ahead of Envoy's answer to it.
	if pm, ok := m.(proto.Message); ok {
		s.c.Add(pm)
	}
	return s.ServerStream.SendMsg(m)
}
