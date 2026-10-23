// Package match has composable matchers for xDS resources, recorded messages, the order things happened in, and what
// Envoy holds. Message matchers work the same for SotW and delta.
//
//	err := match.InOrder(c,
//		match.AckOf(match.Response(xdstest.RDS, match.Containing(match.RouteConfig(match.Not(match.RoutingTo("bar")))))),
//		match.Response(xdstest.CDS, match.Removing("bar")))
package match

import (
	"fmt"
	"slices"
	"strings"
)

// Matcher matches values of type T, and says what it matches.
type Matcher[T any] struct {
	desc  string
	match func(T) bool
	ack   *Matcher[T] // set by AckOf
}

// New makes a matcher from a description and a predicate.
func New[T any](description string, match func(T) bool) Matcher[T] {
	return Matcher[T]{desc: description, match: match}
}

// Match reports whether v matches.
func (m Matcher[T]) Match(v T) bool {
	return m.match(v)
}

func (m Matcher[T]) String() string {
	return m.desc
}

// Not matches what m doesn't.
func Not[T any](m Matcher[T]) Matcher[T] {
	return New("not "+m.desc, func(v T) bool { return !m.Match(v) })
}

// AllOf matches what every one of ms matches.
func AllOf[T any](ms ...Matcher[T]) Matcher[T] {
	return New(describe(" and ", ms), func(v T) bool {
		for _, m := range ms {
			if !m.Match(v) {
				return false
			}
		}
		return true
	})
}

// AnyOf matches what any one of ms matches.
func AnyOf[T any](ms ...Matcher[T]) Matcher[T] {
	return New(describe(" or ", ms), func(v T) bool {
		return slices.ContainsFunc(ms, func(m Matcher[T]) bool { return m.Match(v) })
	})
}

func describe[T any](sep string, ms []Matcher[T]) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.desc
	}
	return "(" + strings.Join(parts, sep) + ")"
}

// with describes what and, if there are any, the matchers it must also meet.
func with[T any](what string, ms []Matcher[T]) string {
	if len(ms) == 0 {
		return what
	}
	return fmt.Sprintf("%s %s", what, AllOf(ms...).desc)
}
