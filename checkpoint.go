package xdstest

import (
	"context"
	"errors"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Checkpoint is where a conversation and Envoy were at one moment: the conversation's size, Envoy's log length and
// Envoy's stats. Take one once Envoy has synced, make changes, then check that everything since was sane:
//
//	cp, err := xdstest.TakeCheckpoint(ctx, envoy, c)
//	controlPlane.AddService("foo")
//	err = xdstest.WaitFor(30*time.Second, xdstest.SettledAfter(c, cp.Message, xdstest.Settled(c, xdstest.DefaultQuietPeriod)))
//	err = cp.Sane(ctx)
type Checkpoint struct {
	envoy        *Envoy
	conversation *Conversation
	// Message is the conversation's size when taken, e.g. for SettledAfter.
	Message int
	// LogLine is Envoy's log length when taken.
	LogLine int
	// Stats are Envoy's stats when taken.
	Stats map[string]uint64
}

// TakeCheckpoint marks where things are now.
func TakeCheckpoint(ctx context.Context, envoy *Envoy, c *Conversation) (*Checkpoint, error) {
	stats, err := envoy.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return &Checkpoint{envoy: envoy, conversation: c, Message: c.Size(), LogLine: envoy.LogSize(), Stats: stats}, nil
}

// Sane checks everything that should hold once things have settled after changes since the checkpoint. Envoy ACKed
// every response and was sent nothing it didn't ask for. Since the checkpoint: no empty responses, make before break,
// and EnvoyHealthy.
func (cp *Checkpoint) Sane(ctx context.Context) error {
	checks := ChecksFor(cp.conversation)
	if err := errors.Join(checks.NothingSentUnasked(false), checks.AllWereAcked(),
		checks.SentNoEmptyResponses(cp.Message), checks.WasMakeBeforeBreak(cp.Message)); err != nil {
		return err
	}
	return cp.EnvoyHealthy(ctx)
}

// SaneAfterReconnect is Sane for a conversation Envoy started by reconnecting. It allows Envoy before 1.19 resending
// resource names for wildcard types. Given before, the previous conversation with this Envoy, it also checks make
// before break from the start of this one, starting from what Envoy last ACKed there: Envoy keeps its config across a
// disconnect. With before nil, make before break isn't checked.
func (cp *Checkpoint) SaneAfterReconnect(ctx context.Context, before *Conversation) error {
	checks := ChecksFor(cp.conversation)
	err := errors.Join(checks.NothingSentUnasked(cp.envoyMinorVersion() < 19), checks.AllWereAcked(), checks.SentNoEmptyResponses(cp.Message))
	if before != nil {
		err = errors.Join(err, checks.WasMakeBeforeBreakAfter(before, 0))
	}
	if err != nil {
		return err
	}
	return cp.EnvoyHealthy(ctx)
}

// EnvoyHealthy checks that Envoy is connected, has nothing warming, and since the checkpoint rejected nothing, timed
// out no initial fetches and logged no warnings or errors.
func (cp *Checkpoint) EnvoyHealthy(ctx context.Context) error {
	if err := cp.envoy.Healthy(ctx, cp.Stats); err != nil {
		return err
	}
	return cp.envoy.WarningsSince(cp.LogLine)
}

// envoyMinorVersion is from the node in Envoy's first request, or a recent version if it didn't say.
func (cp *Checkpoint) envoyMinorVersion() uint32 {
	for _, m := range cp.conversation.Messages() {
		switch r := m.(type) {
		case *discoveryv3.DiscoveryRequest:
			if v := r.GetNode().GetUserAgentBuildVersion(); v != nil {
				return v.GetVersion().GetMinorNumber()
			}
		case *discoveryv3.DeltaDiscoveryRequest:
			if v := r.GetNode().GetUserAgentBuildVersion(); v != nil {
				return v.GetVersion().GetMinorNumber()
			}
		}
	}
	return ^uint32(0)
}
