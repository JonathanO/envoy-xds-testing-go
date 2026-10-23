package match

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
)

// AckOf is Envoy's ACK of a response that matches. Only for InOrder, where the step's position is the ACK's.
func AckOf(response Matcher[proto.Message]) Matcher[proto.Message] {
	m := New("an ACK of "+response.desc, func(proto.Message) bool { return false })
	m.ack = &response
	return m
}

// HasMessage checks the conversation has a message that m matches.
func HasMessage(c *xdstest.Conversation, m Matcher[proto.Message]) error {
	if slices.ContainsFunc(c.Messages(), m.Match) {
		return nil
	}
	return fmt.Errorf("no %s in %d messages", m, c.Size())
}

// InOrder checks each step happened, each later in the conversation than the one before. A step is a message matcher,
// or AckOf one.
func InOrder(c *xdstest.Conversation, steps ...Matcher[proto.Message]) error {
	messages := c.Messages()
	position := -1
	for i, step := range steps {
		var found int
		if step.ack != nil {
			found = ackIndex(messages, position+1, *step.ack)
		} else {
			found = index(messages, position+1, step.Match)
		}
		if found < 0 {
			after := ""
			if position >= 0 {
				after = fmt.Sprintf(" after message %d", position)
			}
			return fmt.Errorf("step %d, %s, not found%s of %d", i+1, step, after, len(messages))
		}
		position = found
	}
	return nil
}

func index(messages []proto.Message, from int, match func(proto.Message) bool) int {
	for i := from; i < len(messages); i++ {
		if match(messages[i]) {
			return i
		}
	}
	return -1
}

// ackIndex is the earliest ACK, from `from` on, of a response that matches.
func ackIndex(messages []proto.Message, from int, response Matcher[proto.Message]) int {
	earliest := -1
	for i, m := range messages {
		r, ok := responseView(m)
		if !ok || !response.Match(m) {
			continue
		}
		ack := index(messages, max(i+1, from), func(m proto.Message) bool {
			q, ok := requestView(m)
			return ok && !q.Nack && q.ResponseNonce == r.Nonce
		})
		if ack >= 0 && (earliest < 0 || ack < earliest) {
			earliest = ack
		}
	}
	return earliest
}
