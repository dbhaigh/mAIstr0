//go:build windows

package fileguard

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestAcePrincipalRecognizesBroadPrincipalsInAllowedACEVariants(t *testing.T) {
	broadSID, err := windows.StringToSid("S-1-1-0")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		aceType     byte
		objectFlags uint32
	}{
		{name: "standard", aceType: windows.ACCESS_ALLOWED_ACE_TYPE},
		{name: "compound", aceType: accessAllowedCompoundACE},
		{name: "object", aceType: accessAllowedObjectACE, objectFlags: objectTypePresent | inheritedObjectTypePresent},
		{name: "callback", aceType: accessAllowedCallbackACE},
		{name: "callback object", aceType: accessAllowedCallbackObjectACE, objectFlags: objectTypePresent},
	} {
		t.Run(test.name, func(t *testing.T) {
			sidOffset := 8
			if test.aceType == accessAllowedCompoundACE {
				sidOffset = 12
			}
			if test.aceType == accessAllowedObjectACE || test.aceType == accessAllowedCallbackObjectACE {
				sidOffset = 12
				if test.objectFlags&objectTypePresent != 0 {
					sidOffset += 16
				}
				if test.objectFlags&inheritedObjectTypePresent != 0 {
					sidOffset += 16
				}
			}
			sidBytes := unsafe.Slice((*byte)(unsafe.Pointer(broadSID)), broadSID.Len())
			aceBytes := make([]byte, sidOffset+len(sidBytes))
			aceBytes[0] = test.aceType
			binary.LittleEndian.PutUint16(aceBytes[2:4], uint16(len(aceBytes)))
			if test.objectFlags != 0 {
				binary.LittleEndian.PutUint32(aceBytes[8:12], test.objectFlags)
			}
			copy(aceBytes[sidOffset:], sidBytes)
			ace := (*windows.ACCESS_ALLOWED_ACE)(unsafe.Pointer(&aceBytes[0]))
			principal, allowed, err := acePrincipal(ace)
			if err != nil {
				t.Fatal(err)
			}
			if !allowed || !isBroadPrincipal(principal) {
				t.Fatal("broad principal was not detected")
			}
		})
	}
}

func TestIsBroadPrincipal(t *testing.T) {
	for _, sidString := range []string{
		"S-1-1-0",
		"S-1-5-11",
		"S-1-5-32-545",
	} {
		sid, err := windows.StringToSid(sidString)
		if err != nil {
			t.Fatal(err)
		}
		if !isBroadPrincipal(sid) {
			t.Errorf("SID %s was not recognized as broad", sidString)
		}
	}
	userSID, err := windows.StringToSid("S-1-5-21-1-2-3-1001")
	if err != nil {
		t.Fatal(err)
	}
	if isBroadPrincipal(userSID) {
		t.Fatal("individual user SID was classified as broad")
	}
}
