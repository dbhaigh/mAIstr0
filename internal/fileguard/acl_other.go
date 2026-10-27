//go:build !windows

package fileguard

func HasBroadACL(string) (bool, error) {
	return false, nil
}
