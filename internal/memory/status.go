package memory

// Availability reports whether persistent memory is available to a process.
type Availability struct {
	Available bool   `json:"available"`
	Warning   string `json:"warning,omitempty"`
}

// AvailabilityFor returns a user-safe status without exposing filesystem details.
func AvailabilityFor(storeAvailable bool) Availability {
	status := Availability{Available: storeAvailable}
	if !storeAvailable {
		status.Warning = "Persistent memory is unavailable; conversation history and learned data will not survive restarts."
	}
	return status
}
