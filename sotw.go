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
	stale := map[string]string{}         // by type: a nonce Envoy echoed from its previous stream
	unacked := map[string]bool{}
	for _, m := range s.c.Messages() {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryRequest:
			if nonce := m.GetResponseNonce(); nonce != "" {
				if sent[m.GetTypeUrl()] == nil {
					// If envoy is reconnecting it may reuse nonces from its previous connection.
					sent[m.GetTypeUrl()] = map[string]bool{nonce: true}
					stale[m.GetTypeUrl()] = nonce
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
			if stale[m.GetTypeUrl()] == m.GetNonce() {
				return fmt.Errorf("sent nonce %q for %s, which Envoy had echoed from its previous stream: its requests can't be "+
					"told from ACKs of this response. Make nonces unique across streams", m.GetNonce(), m.GetTypeUrl())
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
	answered := map[string]bool{}
	for i, m := range s.c.Messages() {
		r, ok := m.(*discoveryv3.DiscoveryResponse)
		if !ok {
			continue
		}
		// A type's first answer may be empty: it tells Envoy there's nothing yet.
		first := !answered[r.GetTypeUrl()]
		answered[r.GetTypeUrl()] = true
		// An empty response is meaningful for wildcard types: it removes everything.
		if !first && i >= fromMessage && len(r.GetResources()) == 0 && !IsWildcard(r.GetTypeUrl()) {
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
	// By "type nonce": Envoy keeps nonces per type, so another type's ACK may reuse one.
	pending := map[string]*discoveryv3.DiscoveryResponse{}
	pendingRemovals := map[string][]string{}
	subscribed := map[string][]string{} // names, by non-wildcard type
	for i, m := range messages {
		switch m := m.(type) {
		case *discoveryv3.DiscoveryResponse:
			// LDS, CDS, SRDS and VHDS are wildcard types: each response is the full set, so anything Envoy has that's missing is removed.
			var removed []string
			if m.GetTypeUrl() == CDS || m.GetTypeUrl() == LDS {
				had := state.clusters
				if m.GetTypeUrl() == LDS {
					had = boolKeys(state.listeners)
				}
				sent := map[string]bool{}
				for _, r := range m.GetResources() {
					name, err := resourceName(r)
					if err != nil {
						return err
					}
					sent[name] = true
				}
				for c := range had {
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
			pending[m.GetTypeUrl()+" "+m.GetNonce()] = m
			pendingRemovals[m.GetTypeUrl()+" "+m.GetNonce()] = removed
		case *discoveryv3.DiscoveryRequest:
			key := m.GetTypeUrl() + " " + m.GetResponseNonce()
			if acked, ok := pending[key]; ok && m.GetErrorDetail() == nil {
				if err := state.acked(acked.GetTypeUrl(), acked.GetResources(), pendingRemovals[key]); err != nil {
					return err
				}
				delete(pending, key)
			}
			if m.GetErrorDetail() == nil && !IsWildcard(m.GetTypeUrl()) {
				// Envoy drops what it stops asking for, e.g. a route config once no listener uses it.
				var dropped []string
				for _, name := range subscribed[m.GetTypeUrl()] {
					if !slices.Contains(m.GetResourceNames(), name) {
						dropped = append(dropped, name)
					}
				}
				subscribed[m.GetTypeUrl()] = m.GetResourceNames()
				if err := state.acked(m.GetTypeUrl(), nil, dropped); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s sotw) AskedForEveryRouteConfigInUse() error {
	state := newAckedState()
	if err := replaySotw(state, s.c.Messages(), -1); err != nil {
		return err
	}
	asked := map[string]bool{} // by Envoy's last RDS request, which lists them all
	for _, m := range s.c.Messages() {
		if r, ok := m.(*discoveryv3.DiscoveryRequest); ok && r.GetTypeUrl() == RDS {
			asked = map[string]bool{}
			for _, name := range r.GetResourceNames() {
				asked[name] = true
			}
		}
	}
	return state.askedForRouteConfigsInUse(asked)
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
