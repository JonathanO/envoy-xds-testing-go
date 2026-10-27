package xdstest

import (
	"fmt"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// DefaultQuietPeriod is how long a conversation must be silent to count as settled, if the control plane can't say.
const DefaultQuietPeriod = 500 * time.Millisecond

// Settled returns a condition, for [WaitFor], that every response was answered and nothing has been sent either way
// for quietPeriod.
func Settled(c *Conversation, quietPeriod time.Duration) func() error {
	return func() error {
		if err := allAnswered(c); err != nil {
			return err
		}
		if !c.QuietFor(quietPeriod) {
			return fmt.Errorf("conversation not quiet for %v yet", quietPeriod)
		}
		return nil
	}
}

// SettledIdle returns a condition that the control plane says it is idle and every response was answered. Use it if
// your control plane can tell whether it has anything in flight or waiting to be sent on the stream: it knows
// whether more is coming, and there's no quiet period to wait out.
func SettledIdle(c *Conversation, idle func() bool) func() error {
	return func() error {
		if !idle() {
			return fmt.Errorf("control plane not idle")
		}
		return allAnswered(c)
	}
}

// SettledAfter returns a condition that something was sent after fromMessage, then settled. Use it to wait for a
// change: when you start waiting, the control plane may not have begun sending it yet, and look idle.
func SettledAfter(c *Conversation, fromMessage int, settled func() error) func() error {
	return func() error {
		sent := false
		for _, m := range c.Messages()[fromMessage:] {
			switch m.(type) {
			case *discoveryv3.DiscoveryResponse, *discoveryv3.DeltaDiscoveryResponse:
				sent = true
			}
		}
		if !sent {
			return fmt.Errorf("nothing sent since message %d", fromMessage)
		}
		return settled()
	}
}

// WaitFor polls check until it returns nil, or returns its last error once timeout has passed.
func WaitFor(timeout time.Duration, check func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := check()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %v: %w", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// allAnswered checks that every response nonce was ACKed or NACKed.
func allAnswered(c *Conversation) error {
	// By type: Envoy keeps nonces per type, so a library may number them per type.
	type typeNonce struct{ typeURL, nonce string }
	unanswered := map[typeNonce]bool{}
	for _, m := range c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryResponse:
			unanswered[typeNonce{m.GetTypeUrl(), m.GetNonce()}] = true
		case *discoveryv3.DeltaDiscoveryResponse:
			unanswered[typeNonce{m.GetTypeUrl(), m.GetNonce()}] = true
		case *discoveryv3.DiscoveryRequest:
			delete(unanswered, typeNonce{m.GetTypeUrl(), m.GetResponseNonce()})
		case *discoveryv3.DeltaDiscoveryRequest:
			delete(unanswered, typeNonce{m.GetTypeUrl(), m.GetResponseNonce()})
		}
	}
	if len(unanswered) > 0 {
		return fmt.Errorf("%d responses not answered yet", len(unanswered))
	}
	return nil
}
