package xdstest

import (
	"testing"
	"time"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

func TestConversationAloneSettlesOnceEverythingIsAnsweredAndQuiet(t *testing.T) {
	c := NewConversation()
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: CDS})
	c.Add(&discoveryv3.DiscoveryResponse{TypeUrl: CDS, Nonce: "1"})
	time.Sleep(30 * time.Millisecond)
	if Settled(c, 20*time.Millisecond)() == nil {
		t.Error("settled with a response unanswered")
	}

	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"})
	if Settled(c, time.Minute)() == nil {
		t.Error("settled before the quiet period")
	}
	time.Sleep(30 * time.Millisecond)
	if err := Settled(c, 20*time.Millisecond)(); err != nil {
		t.Error(err)
	}
}

func TestIdleHookMustBeIdleAndEverythingAnswered(t *testing.T) {
	c := NewConversation()
	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: CDS, Nonce: "1"})
	if SettledIdle(c, func() bool { return true })() == nil {
		t.Error("settled with a response unanswered")
	}

	c.Add(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"})
	if SettledIdle(c, func() bool { return false })() == nil {
		t.Error("settled while the control plane is busy")
	}
	if err := SettledIdle(c, func() bool { return true })(); err != nil {
		t.Error(err, "(no quiet period needed)")
	}
}

func TestNoncesAreAnsweredPerType(t *testing.T) {
	c := NewConversation()
	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: CDS, Nonce: "1"})
	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: EDS, Nonce: "1"})
	c.Add(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"})
	if SettledIdle(c, func() bool { return true })() == nil {
		t.Error("settled with EDS unanswered, as CDS used the same nonce")
	}

	c.Add(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: EDS, ResponseNonce: "1"})
	if err := SettledIdle(c, func() bool { return true })(); err != nil {
		t.Error(err)
	}
}

func TestSettledAfterWaitsForSomethingToBeSent(t *testing.T) {
	c := NewConversation()
	mark := c.Size()
	idle := SettledIdle(c, func() bool { return true })
	if SettledAfter(c, mark, idle)() == nil {
		t.Error("settled with nothing sent yet")
	}

	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: CDS, Nonce: "1"})
	c.Add(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"})
	if err := SettledAfter(c, mark, idle)(); err != nil {
		t.Error(err)
	}
}
