// Package closeutil provides helpers for releasing resources whose close
// errors cannot be acted upon, such as read-only HTTP response bodies.
package closeutil

import "io"

// Discard closes c and intentionally ignores the error. Use it in deferred
// calls (defer closeutil.Discard(resp.Body)) where the resource was only read
// and a close failure cannot change the outcome. The closer is evaluated when
// the defer statement runs, matching defer c.Close().
func Discard(c io.Closer) {
	_ = c.Close()
}
