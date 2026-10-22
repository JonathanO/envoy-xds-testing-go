package xdstest

import (
	"fmt"
	"sort"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
)

// Checks are checks on one recorded conversation. Each looks at the conversation as it is when called, and returns
// nil or what is wrong. Get them with [Sotw] or [Delta], whichever protocol Envoy uses.
type Checks interface {
	// AllWereAcked checks that Envoy ACKed every response, and NACKed none.
	AllWereAcked() error

	// NothingSentUnasked checks that every response is for a type, and resource names, that Envoy subscribed to.
	// Before 1.19, Envoy resent resource names for wildcard types when it reconnected; pass true to allow that.
	NothingSentUnasked(allowPre119WildcardReconnect bool) error

	// UnsubscribedFrom checks that Envoy dropped its subscription to a resource, e.g. EDS for a removed cluster.
	UnsubscribedFrom(typeURL, name string) error

	// SentNoEmptyResponses checks that no response from fromMessage on was empty. Earlier ones may be: an empty first
	// response for a wildcard type tells Envoy there is nothing yet.
	SentNoEmptyResponses(fromMessage int) error

	// WasMakeBeforeBreak checks that, from fromMessage on, no route was sent before Envoy ACKed the clusters and
	// endpoints it uses, and no cluster was removed while an ACKed route still used it. Start after the initial sync:
	// answers to Envoy's first subscriptions aren't ordered, Envoy's own startup sequencing covers those.
	WasMakeBeforeBreak(fromMessage int) error

	// AnsweredEveryType checks that every type Envoy asked for got at least one response. Without one Envoy waits out
	// its initial fetch timeout, or forever if that is disabled.
	AnsweredEveryType() error

	// HadConversationsWith checks that Envoy asked for each of these types.
	HadConversationsWith(typeURLs ...string) error
}

func requestType(m proto.Message) (string, bool) {
	switch m := m.(type) {
	case *discoveryv3.DiscoveryRequest:
		return m.GetTypeUrl(), true
	case *discoveryv3.DeltaDiscoveryRequest:
		return m.GetTypeUrl(), true
	}
	return "", false
}

func responseType(m proto.Message) (string, bool) {
	switch m := m.(type) {
	case *discoveryv3.DiscoveryResponse:
		return m.GetTypeUrl(), true
	case *discoveryv3.DeltaDiscoveryResponse:
		return m.GetTypeUrl(), true
	}
	return "", false
}

func answeredEveryType(c *Conversation) error {
	unanswered := map[string]bool{}
	messages := c.Messages()
	for _, m := range messages {
		if t, ok := requestType(m); ok {
			unanswered[t] = true
		}
	}
	for _, m := range messages {
		if t, ok := responseType(m); ok {
			delete(unanswered, t)
		}
	}
	if len(unanswered) > 0 {
		return fmt.Errorf("envoy got no response at all for %v", sortedKeys(unanswered))
	}
	return nil
}

func hadConversationsWith(c *Conversation, typeURLs []string) error {
	needed := map[string]bool{}
	for _, t := range typeURLs {
		needed[t] = true
	}
	for _, m := range c.Messages() {
		if t, ok := requestType(m); ok {
			delete(needed, t)
		}
	}
	if len(needed) > 0 {
		return fmt.Errorf("envoy never asked for %v", sortedKeys(needed))
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func unexpected(m proto.Message) error {
	return fmt.Errorf("unexpected message type %T", m)
}
