package sdk

import "fmt"

// Target identifies a handler to invoke. Service and Handler must be
// non-empty. Key is required for keyed services (Virtual Objects) and
// must be empty for unkeyed services.
type Target struct {
	Service string
	Handler string
	Key     string
}

// String returns a stable rendering useful for logs and metrics labels.
// Examples:
//   - "Greeter/hello"            (unkeyed)
//   - "Cart[user-42]/checkout"   (keyed)
func (t Target) String() string {
	if t.Key == "" {
		return fmt.Sprintf("%s/%s", t.Service, t.Handler)
	}
	return fmt.Sprintf("%s[%s]/%s", t.Service, t.Key, t.Handler)
}
