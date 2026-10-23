package match

import (
	"fmt"
	"slices"
	"strings"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
)

// ResponseView is a response to Envoy, the same whether it was SotW or delta.
type ResponseView struct {
	TypeURL   string
	Nonce     string
	Resources []proto.Message // decoded, if their type is linked in
	Removed   []string        // delta only
	Delta     bool
}

// RequestView is a request from Envoy, the same whether it was SotW or delta.
type RequestView struct {
	TypeURL       string
	ResponseNonce string
	Subscribe     []string // delta's subscribe list, or SotW's whole resource name list
	Unsubscribe   []string // delta only: SotW requests carry the whole list instead
	Error         string   // why Envoy rejected the response this answers
	Nack          bool
}

// Response matches a response of this type ("" for any) that all of ms match.
func Response(typeURL string, ms ...Matcher[ResponseView]) Matcher[proto.Message] {
	all := AllOf(ms...)
	return New(with(typed("a response", typeURL), ms), func(m proto.Message) bool {
		r, ok := responseView(m)
		return ok && (typeURL == "" || r.TypeURL == typeURL) && all.Match(r)
	})
}

// Request matches a request of this type ("" for any) that all of ms match.
func Request(typeURL string, ms ...Matcher[RequestView]) Matcher[proto.Message] {
	all := AllOf(ms...)
	return New(with(typed("a request", typeURL), ms), func(m proto.Message) bool {
		r, ok := requestView(m)
		return ok && (typeURL == "" || r.TypeURL == typeURL) && all.Match(r)
	})
}

// Nack matches Envoy rejecting a response of this type ("" for any), where all of ms match.
func Nack(typeURL string, ms ...Matcher[RequestView]) Matcher[proto.Message] {
	all := AllOf(ms...)
	return New(with(typed("a NACK", typeURL), ms), func(m proto.Message) bool {
		r, ok := requestView(m)
		return ok && r.Nack && (typeURL == "" || r.TypeURL == typeURL) && all.Match(r)
	})
}

// Containing matches a response carrying a resource that m matches.
func Containing(m Matcher[proto.Message]) Matcher[ResponseView] {
	return New("containing "+m.desc, func(r ResponseView) bool { return slices.ContainsFunc(r.Resources, m.Match) })
}

// Removing matches a response that removes this resource. For delta, it's in removed_resources. For SotW, only
// wildcard types (LDS, CDS) can remove one, by leaving it out, so this matches a wildcard response without it.
func Removing(name string) Matcher[ResponseView] {
	return New(fmt.Sprintf("removing %q", name), func(r ResponseView) bool {
		if r.Delta {
			return slices.Contains(r.Removed, name)
		}
		return xdstest.IsWildcard(r.TypeURL) && !slices.ContainsFunc(r.Resources, func(m proto.Message) bool { return xdstest.NameOf(m) == name })
	})
}

// SubscribingTo matches a request subscribing to this name.
func SubscribingTo(name string) Matcher[RequestView] {
	return New(fmt.Sprintf("subscribing to %q", name), func(r RequestView) bool { return slices.Contains(r.Subscribe, name) })
}

// UnsubscribingFrom matches a delta request unsubscribing from this name. SotW requests list every name, so for SotW use
// Checks.UnsubscribedFrom on the conversation.
func UnsubscribingFrom(name string) Matcher[RequestView] {
	return New(fmt.Sprintf("unsubscribing from %q", name), func(r RequestView) bool { return slices.Contains(r.Unsubscribe, name) })
}

// WithMessage matches a NACK whose error contains this text.
func WithMessage(text string) Matcher[RequestView] {
	return New(fmt.Sprintf("with error containing %q", text), func(r RequestView) bool { return strings.Contains(r.Error, text) })
}

func typed(what, typeURL string) string {
	if typeURL == "" {
		return what
	}
	return fmt.Sprintf("%s for %s", what, typeURL[strings.LastIndex(typeURL, ".")+1:])
}

func responseView(m proto.Message) (ResponseView, bool) {
	switch r := m.(type) {
	case *discoveryv3.DiscoveryResponse:
		return ResponseView{TypeURL: r.GetTypeUrl(), Nonce: r.GetNonce(), Resources: decode(r.GetResources())}, true
	case *discoveryv3.DeltaDiscoveryResponse:
		anys := make([]*anypb.Any, len(r.GetResources()))
		for i, res := range r.GetResources() {
			anys[i] = res.GetResource()
		}
		return ResponseView{TypeURL: r.GetTypeUrl(), Nonce: r.GetNonce(), Resources: decode(anys), Removed: r.GetRemovedResources(), Delta: true}, true
	}
	return ResponseView{}, false
}

func requestView(m proto.Message) (RequestView, bool) {
	switch r := m.(type) {
	case *discoveryv3.DiscoveryRequest:
		return RequestView{TypeURL: r.GetTypeUrl(), ResponseNonce: r.GetResponseNonce(), Subscribe: r.GetResourceNames(),
			Error: r.GetErrorDetail().GetMessage(), Nack: r.GetErrorDetail() != nil}, true
	case *discoveryv3.DeltaDiscoveryRequest:
		return RequestView{TypeURL: r.GetTypeUrl(), ResponseNonce: r.GetResponseNonce(), Subscribe: r.GetResourceNamesSubscribe(),
			Unsubscribe: r.GetResourceNamesUnsubscribe(), Error: r.GetErrorDetail().GetMessage(), Nack: r.GetErrorDetail() != nil}, true
	}
	return RequestView{}, false
}

func decode(anys []*anypb.Any) []proto.Message {
	decoded := make([]proto.Message, 0, len(anys))
	for _, a := range anys {
		if m, err := a.UnmarshalNew(); err == nil {
			decoded = append(decoded, m)
		} else {
			decoded = append(decoded, a)
		}
	}
	return decoded
}
