package xdstest

import (
	"fmt"
	"slices"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Sotw returns checks for a state-of-the-world conversation.
func Sotw(c *Conversation) Checks {
	return sotw{c}
}

type sotw struct {
	c *Conversation
}

func (s sotw) AllWereAcked() error {
	sent := map[string]map[string]bool{} // nonces by type
	unacked := map[string]bool{}
	for _, m := range s.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryRequest:
			if nonce := m.GetResponseNonce(); nonce != "" {
				if sent[m.GetTypeUrl()] == nil {
					// If envoy is reconnecting it may reuse nonces from its previous connection.
					sent[m.GetTypeUrl()] = map[string]bool{nonce: true}
				} else if !sent[m.GetTypeUrl()][nonce] {
					return fmt.Errorf("unexpected response nonce %q for %s", nonce, m.GetTypeUrl())
				}
				delete(unacked, m.GetTypeUrl()+" "+nonce)
			}
			if m.GetErrorDetail() != nil {
				return fmt.Errorf("envoy sent error %q", m.GetErrorDetail().GetMessage())
			}
		case *discoveryv3.DiscoveryResponse:
			if sent[m.GetTypeUrl()] == nil {
				sent[m.GetTypeUrl()] = map[string]bool{}
			}
			if sent[m.GetTypeUrl()][m.GetNonce()] {
				return fmt.Errorf("sent nonce %q for %s more than once", m.GetNonce(), m.GetTypeUrl())
			}
			sent[m.GetTypeUrl()][m.GetNonce()] = true
			unacked[m.GetTypeUrl()+" "+m.GetNonce()] = true
		default:
			return unexpected(m)
		}
	}
	if len(unacked) > 0 {
		return fmt.Errorf("unacked responses (type nonce): %v", sortedKeys(unacked))
	}
	return nil
}

func (s sotw) NothingSentUnasked(allowPre119WildcardReconnect bool) error {
	subs := map[string][]string{}
	wildcards := map[string]bool{}
	seen := map[string]bool{}
	seenSubs := map[string]bool{}
	for _, m := range s.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryRequest:
			t := m.GetTypeUrl()
			requested := m.GetResourceNames()
			if !seen[t] && IsWildcard(t) && allowPre119WildcardReconnect {
				// First request, allow bad behaviour from old versions.
				requested = nil
			}
			seen[t] = true
			if len(requested) == 0 && !seenSubs[t] {
				wildcards[t] = true // legacy wildcard
				continue
			}
			// This type has asked for names, so an empty list from now on means none, not wildcard.
			seenSubs[t] = true
			if slices.Contains(requested, "*") {
				wildcards[t] = true
				requested = slices.DeleteFunc(slices.Clone(requested), func(n string) bool { return n == "*" })
			} else {
				delete(wildcards, t)
			}
			subs[t] = requested
		case *discoveryv3.DiscoveryResponse:
			t := m.GetTypeUrl()
			if wildcards[t] {
				continue
			}
			names, ok := subs[t]
			if !ok {
				return fmt.Errorf("sent %s, which envoy never asked for", t)
			}
			var unwanted []string
			for _, r := range m.GetResources() {
				name, err := resourceName(r)
				if err != nil {
					return err
				}
				if !slices.Contains(names, name) {
					unwanted = append(unwanted, name)
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

func (s sotw) UnsubscribedFrom(typeURL, name string) error {
	subscribed := false
	for _, m := range s.c.Messages() {
		if r, ok := m.(*discoveryv3.DiscoveryRequest); ok && r.GetTypeUrl() == typeURL {
			if slices.Contains(r.GetResourceNames(), name) {
				subscribed = true
			} else if subscribed {
				return nil
			}
		}
	}
	return fmt.Errorf("envoy never unsubscribed %s from %q", typeURL, name)
}

func (s sotw) SentNoEmptyResponses(fromMessage int) error {
	for _, m := range s.c.Messages()[fromMessage:] {
		// An empty response is meaningful for wildcard types: it removes everything.
		if r, ok := m.(*discoveryv3.DiscoveryResponse); ok && len(r.GetResources()) == 0 && !IsWildcard(r.GetTypeUrl()) {
			return fmt.Errorf("sent empty response %q for %s", r.GetNonce(), r.GetTypeUrl())
		}
	}
	return nil
}

func (s sotw) WasMakeBeforeBreak(fromMessage int) error {
	return s.WasMakeBeforeBreakAfter(nil, fromMessage)
}

func (s sotw) WasMakeBeforeBreakAfter(before *Conversation, fromMessage int) error {
	state := newAckedState()
	if before != nil {
		if err := replaySotw(state, before.Messages(), -1); err != nil {
			return err
		}
	}
	if err := replaySotw(state, s.c.Messages(), fromMessage); err != nil {
		return err
	}
	return state.err()
}

// replaySotw tracks what Envoy ACKs in messages, checking responses from checkFrom on, or none if it's negative.
func replaySotw(state *ackedState, messages []proto.Message, checkFrom int) error {
	pending := map[string]*discoveryv3.DiscoveryResponse{}
	pendingRemovals := map[string][]string{}
	for i, m := range messages {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryResponse:
			// CDS is a wildcard type: each response is the full set, so anything Envoy has that's missing is removed.
			var removed []string
			if m.GetTypeUrl() == CDS {
				sent := map[string]bool{}
				for _, r := range m.GetResources() {
					name, err := resourceName(r)
					if err != nil {
						return err
					}
					sent[name] = true
				}
				for c := range state.clusters {
					if !sent[c] {
						removed = append(removed, c)
					}
				}
			}
			if checkFrom >= 0 && i >= checkFrom {
				if err := state.checkResponse(m.GetTypeUrl(), m.GetResources(), removed); err != nil {
					return err
				}
			}
			pending[m.GetNonce()] = m
			pendingRemovals[m.GetNonce()] = removed
		case *discoveryv3.DiscoveryRequest:
			if acked, ok := pending[m.GetResponseNonce()]; ok && m.GetErrorDetail() == nil {
				if err := state.acked(acked.GetTypeUrl(), acked.GetResources(), pendingRemovals[m.GetResponseNonce()]); err != nil {
					return err
				}
				delete(pending, m.GetResponseNonce())
			}
		}
	}
	return nil
}

func (s sotw) AnsweredEveryType() error {
	return answeredEveryType(s.c)
}

func (s sotw) HadConversationsWith(typeURLs ...string) error {
	return hadConversationsWith(s.c, typeURLs)
}

// resourceName decodes a resource and returns its NameOf.
func resourceName(a *anypb.Any) (string, error) {
	m, err := a.UnmarshalNew()
	if err != nil {
		return "", fmt.Errorf("decoding %s: %w", a.GetTypeUrl(), err)
	}
	if name := NameOf(m); name != "" {
		return name, nil
	}
	return "", fmt.Errorf("can't work out the name of a %s", a.GetTypeUrl())
}
