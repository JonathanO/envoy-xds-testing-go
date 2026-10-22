package xdstest

import (
	"fmt"
	"slices"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
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
	outstanding := map[string]bool{}
	for _, m := range d.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DeltaDiscoveryRequest:
			if nonce := m.GetResponseNonce(); nonce != "" {
				if !outstanding[nonce] {
					return fmt.Errorf("unexpected response nonce %q, expected one of %v", nonce, sortedKeys(outstanding))
				}
				delete(outstanding, nonce)
			}
			if m.GetErrorDetail() != nil {
				return fmt.Errorf("envoy sent error %q", m.GetErrorDetail().GetMessage())
			}
		case *discoveryv3.DeltaDiscoveryResponse:
			if outstanding[m.GetNonce()] {
				return fmt.Errorf("sent nonce %q more than once", m.GetNonce())
			}
			outstanding[m.GetNonce()] = true
		default:
			return unexpected(m)
		}
	}
	if len(outstanding) > 0 {
		return fmt.Errorf("unacked nonces %v", sortedKeys(outstanding))
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
	for _, m := range d.c.Messages()[fromMessage:] {
		if r, ok := m.(*discoveryv3.DeltaDiscoveryResponse); ok && len(r.GetResources()) == 0 && len(r.GetRemovedResources()) == 0 {
			return fmt.Errorf("sent empty response %q for %s", r.GetNonce(), r.GetTypeUrl())
		}
	}
	return nil
}

func (d delta) WasMakeBeforeBreak(fromMessage int) error {
	state := newAckedState()
	pending := map[string]*discoveryv3.DeltaDiscoveryResponse{}
	for i, m := range d.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DeltaDiscoveryResponse:
			if i >= fromMessage {
				if err := state.checkResponse(m.GetTypeUrl(), deltaResources(m), m.GetRemovedResources()); err != nil {
					return err
				}
			}
			pending[m.GetNonce()] = m
		case *discoveryv3.DeltaDiscoveryRequest:
			if acked, ok := pending[m.GetResponseNonce()]; ok && m.GetErrorDetail() == nil {
				if err := state.acked(acked.GetTypeUrl(), deltaResources(acked), acked.GetRemovedResources()); err != nil {
					return err
				}
				delete(pending, m.GetResponseNonce())
			}
		}
	}
	return state.err()
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
