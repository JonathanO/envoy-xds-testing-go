package xdstest

import (
	"fmt"
	"slices"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Delta returns checks for an incremental (delta) conversation.
func Delta(c *Conversation) Checks {
	return delta{c}
}

type delta struct {
	c *Conversation
}

func (d delta) AllWereAcked() error {
	// By "type nonce": Envoy keeps nonces per type, so a library may number them per type.
	sent := map[string]bool{}
	outstanding := map[string]bool{}
	for _, m := range d.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DeltaDiscoveryRequest:
			if nonce := m.GetResponseNonce(); nonce != "" {
				key := m.GetTypeUrl() + " " + nonce
				if !outstanding[key] {
					return fmt.Errorf("unexpected response nonce %q for %s, expected one of (type nonce) %v", nonce, m.GetTypeUrl(), sortedKeys(outstanding))
				}
				delete(outstanding, key)
			}
			if m.GetErrorDetail() != nil {
				return fmt.Errorf("envoy sent error %q", m.GetErrorDetail().GetMessage())
			}
		case *discoveryv3.DeltaDiscoveryResponse:
			key := m.GetTypeUrl() + " " + m.GetNonce()
			if sent[key] {
				return fmt.Errorf("sent nonce %q for %s more than once", m.GetNonce(), m.GetTypeUrl())
			}
			sent[key] = true
			outstanding[key] = true
		default:
			return unexpected(m)
		}
	}
	if len(outstanding) > 0 {
		return fmt.Errorf("unacked responses (type nonce): %v", sortedKeys(outstanding))
	}
	return nil
}

func (d delta) NothingSentUnasked(allowPre119WildcardReconnect bool) error {
	subs := map[string]map[string]bool{}
	wildcards := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range d.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DeltaDiscoveryRequest:
			t := m.GetTypeUrl()
			adds := m.GetResourceNamesSubscribe()
			if !seen[t] && (len(adds) == 0 || (IsWildcard(t) && allowPre119WildcardReconnect)) {
				// First request of this type, allow old wildcard behaviour
				adds = []string{"*"}
			}
			seen[t] = true
			if subs[t] == nil {
				subs[t] = map[string]bool{}
			}
			for _, name := range adds {
				if name == "*" {
					if wildcards[t] {
						return fmt.Errorf("envoy subscribed to %s as wildcard more than once", t)
					}
					wildcards[t] = true
				} else {
					if subs[t][name] {
						return fmt.Errorf("envoy subscribed %s to %q more than once", t, name)
					}
					subs[t][name] = true
				}
			}
			for _, name := range m.GetResourceNamesUnsubscribe() {
				if name == "*" {
					if !wildcards[t] {
						return fmt.Errorf("envoy unsubscribed %s as wildcard when it was not subscribed", t)
					}
					delete(wildcards, t)
				} else {
					if !subs[t][name] {
						return fmt.Errorf("envoy unsubscribed %s from %q when it was not subscribed", t, name)
					}
					delete(subs[t], name)
				}
			}
		case *discoveryv3.DeltaDiscoveryResponse:
			t := m.GetTypeUrl()
			if wildcards[t] {
				continue
			}
			if len(subs[t]) == 0 {
				return fmt.Errorf("sent %s, which envoy never asked for", t)
			}
			var unwanted []string
			for _, r := range m.GetResources() {
				if !subs[t][r.GetName()] {
					unwanted = append(unwanted, r.GetName())
				}
			}
			if len(unwanted) > 0 {
				return fmt.Errorf("sent %s containing %v, which envoy never asked for", t, unwanted)
			}
		default:
			return unexpected(m)
		}
	}
	return nil
}

func (d delta) UnsubscribedFrom(typeURL, name string) error {
	for _, m := range d.c.Messages() {
		if r, ok := m.(*discoveryv3.DeltaDiscoveryRequest); ok && r.GetTypeUrl() == typeURL && slices.Contains(r.GetResourceNamesUnsubscribe(), name) {
			return nil
		}
	}
	return fmt.Errorf("envoy never unsubscribed %s from %q", typeURL, name)
}

func (d delta) SentNoEmptyResponses(fromMessage int) error {
	answered := map[string]bool{}
	for i, m := range d.c.Messages() {
		r, ok := m.(*discoveryv3.DeltaDiscoveryResponse)
		if !ok {
			continue
		}
		// A type's first answer may be empty: it tells Envoy there's nothing yet.
		first := !answered[r.GetTypeUrl()]
		answered[r.GetTypeUrl()] = true
		if !first && i >= fromMessage && len(r.GetResources()) == 0 && len(r.GetRemovedResources()) == 0 {
			return fmt.Errorf("sent empty response %q for %s", r.GetNonce(), r.GetTypeUrl())
		}
	}
	return nil
}

func (d delta) WasMakeBeforeBreak(fromMessage int) error {
	return d.WasMakeBeforeBreakAfter(nil, fromMessage)
}

func (d delta) WasMakeBeforeBreakAfter(before *Conversation, fromMessage int) error {
	state := newAckedState()
	if before != nil {
		if err := replayDelta(state, before.Messages(), -1); err != nil {
			return err
		}
	}
	if err := replayDelta(state, d.c.Messages(), fromMessage); err != nil {
		return err
	}
	return state.err()
}

// replayDelta tracks what Envoy ACKs in messages, checking responses from checkFrom on, or none if it's negative.
func replayDelta(state *ackedState, messages []proto.Message, checkFrom int) error {
	pending := map[string]*discoveryv3.DeltaDiscoveryResponse{} // by "type nonce"
	for i, m := range messages {
		switch m := m.(type) {
		case *discoveryv3.DeltaDiscoveryResponse:
			if checkFrom >= 0 && i >= checkFrom {
				if err := state.checkResponse(m.GetTypeUrl(), deltaResources(m), m.GetRemovedResources()); err != nil {
					return err
				}
			}
			pending[m.GetTypeUrl()+" "+m.GetNonce()] = m
		case *discoveryv3.DeltaDiscoveryRequest:
			key := m.GetTypeUrl() + " " + m.GetResponseNonce()
			if acked, ok := pending[key]; ok && m.GetErrorDetail() == nil {
				if err := state.acked(acked.GetTypeUrl(), deltaResources(acked), acked.GetRemovedResources()); err != nil {
					return err
				}
				delete(pending, key)
			}
			if m.GetErrorDetail() == nil {
				// Envoy drops what it unsubscribes from, e.g. a route config once no listener uses it.
				if err := state.acked(m.GetTypeUrl(), nil, m.GetResourceNamesUnsubscribe()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d delta) AskedForEveryRouteConfigInUse() error {
	state := newAckedState()
	if err := replayDelta(state, d.c.Messages(), -1); err != nil {
		return err
	}
	asked := map[string]bool{}
	for _, m := range d.c.Messages() {
		if r, ok := m.(*discoveryv3.DeltaDiscoveryRequest); ok && r.GetTypeUrl() == RDS {
			for _, name := range r.GetResourceNamesSubscribe() {
				asked[name] = true
			}
			for _, name := range r.GetResourceNamesUnsubscribe() {
				delete(asked, name)
			}
		}
	}
	return state.askedForRouteConfigsInUse(asked)
}

func (d delta) AnsweredEveryType() error {
	return answeredEveryType(d.c)
}

func (d delta) HadConversationsWith(typeURLs ...string) error {
	return hadConversationsWith(d.c, typeURLs)
}

func deltaResources(r *discoveryv3.DeltaDiscoveryResponse) []*anypb.Any {
	resources := make([]*anypb.Any, 0, len(r.GetResources()))
	for _, res := range r.GetResources() {
		resources = append(resources, res.GetResource())
	}
	return resources
}
