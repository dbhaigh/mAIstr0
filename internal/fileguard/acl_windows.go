//go:build windows

package fileguard

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	accessAllowedCompoundACE       = 4
	accessAllowedObjectACE         = 5
	accessAllowedCallbackACE       = 9
	accessAllowedCallbackObjectACE = 11
	objectTypePresent              = 1
	inheritedObjectTypePresent     = 2
)

// HasBroadACL reports whether a Windows file DACL grants access to a broad
// built-in principal. It does not modify the file's ACL.
func HasBroadACL(path string) (bool, error) {
	descriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return false, fmt.Errorf("read Windows ACL: %w", err)
	}
	if descriptor == nil {
		return true, nil
	}
	acl, _, err := descriptor.DACL()
	if err != nil {
		return false, fmt.Errorf("read Windows file DACL: %w", err)
	}
	if acl == nil {
		return true, nil
	}
	for index := uint32(0); index < uint32(acl.AceCount); index++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, index, &ace); err != nil {
			return false, fmt.Errorf("inspect Windows file DACL entry: %w", err)
		}
		principal, allowed, err := acePrincipal(ace)
		if err != nil {
			return false, fmt.Errorf("inspect Windows file DACL entry %d: %w", index, err)
		}
		if allowed && isBroadPrincipal(principal) {
			return true, nil
		}
	}
	return false, nil
}

func acePrincipal(ace *windows.ACCESS_ALLOWED_ACE) (*windows.SID, bool, error) {
	var sidOffset int
	switch ace.Header.AceType {
	case windows.ACCESS_ALLOWED_ACE_TYPE, accessAllowedCallbackACE:
		sidOffset = 8
	case accessAllowedCompoundACE:
		sidOffset = 12
	case accessAllowedObjectACE, accessAllowedCallbackObjectACE:
		aceBytes := unsafe.Slice((*byte)(unsafe.Pointer(ace)), int(ace.Header.AceSize))
		if len(aceBytes) < 12 {
			return nil, false, fmt.Errorf("truncated object access entry")
		}
		objectFlags := binary.LittleEndian.Uint32(aceBytes[8:12])
		if objectFlags&^uint32(3) != 0 {
			return nil, false, fmt.Errorf("unsupported object access flags")
		}
		sidOffset = 12
		if objectFlags&objectTypePresent != 0 {
			sidOffset += 16
		}
		if objectFlags&inheritedObjectTypePresent != 0 {
			sidOffset += 16
		}
	default:
		return nil, false, nil
	}
	aceBytes := unsafe.Slice((*byte)(unsafe.Pointer(ace)), int(ace.Header.AceSize))
	if len(aceBytes) < sidOffset+8 {
		return nil, false, fmt.Errorf("truncated access entry SID")
	}
	subAuthorities := int(aceBytes[sidOffset+1])
	if subAuthorities > 15 || len(aceBytes) < sidOffset+8+subAuthorities*4 {
		return nil, false, fmt.Errorf("invalid access entry SID")
	}
	return (*windows.SID)(unsafe.Pointer(&aceBytes[sidOffset])), true, nil
}

func isBroadPrincipal(principal *windows.SID) bool {
	if principal == nil {
		return false
	}
	for _, sidType := range []windows.WELL_KNOWN_SID_TYPE{
		windows.WinWorldSid,
		windows.WinAuthenticatedUserSid,
		windows.WinBuiltinUsersSid,
		windows.WinBuiltinGuestsSid,
		windows.WinInteractiveSid,
		windows.WinNetworkSid,
		windows.WinAnonymousSid,
	} {
		if principal.IsWellKnown(sidType) {
			return true
		}
	}
	return false
}
