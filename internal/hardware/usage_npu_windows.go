//go:build windows

package hardware

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const windowsNPUCounterScript = `
$ErrorActionPreference = 'Stop'
$adapterLUIDs = @(%s)
$utilizationByAdapter = @()
foreach ($adapterLUID in $adapterLUIDs) {
  $filter = "Name LIKE '%%$adapterLUID%%' AND Name LIKE '%%engtype_compute%%'"
  $samples = @(Get-CimInstance -ClassName Win32_PerfFormattedData_GPUPerformanceCounters_GPUEngine -Filter $filter -ErrorAction Stop |
    ForEach-Object { [double]$_.UtilizationPercentage } |
    Where-Object { $_ -ge 0 -and $_ -le 100 })
  if ($samples.Count -gt 0) {
    $total = ($samples | Measure-Object -Sum).Sum
    $utilizationByAdapter += [math]::Min(100, $total)
  } else {
    $utilizationByAdapter += 0
  }
}
$maximum = ($utilizationByAdapter | Measure-Object -Maximum).Maximum
[Console]::WriteLine(([double]$maximum).ToString('F1', [Globalization.CultureInfo]::InvariantCulture))
`

type dxcoreGUID struct {
	data1 uint32
	data2 uint16
	data3 uint16
	data4 [8]byte
}

type dxcoreLUID struct {
	lowPart  uint32
	highPart int32
}

var (
	iidDXCoreAdapterFactory = dxcoreGUID{
		data1: 0x78ee5945, data2: 0xc36e, data3: 0x4b13,
		data4: [8]byte{0xa6, 0x69, 0x00, 0x5d, 0xd1, 0x1c, 0x0f, 0x06},
	}
	iidDXCoreAdapterList = dxcoreGUID{
		data1: 0x526c7776, data2: 0x40e9, data3: 0x459b,
		data4: [8]byte{0xb7, 0x11, 0xf3, 0x2a, 0xd7, 0x6d, 0xfc, 0x28},
	}
	iidDXCoreAdapter = dxcoreGUID{
		data1: 0xf0db4c7f, data2: 0xfe5a, data3: 0x42a2,
		data4: [8]byte{0xbd, 0x62, 0xf2, 0xa6, 0xcf, 0x6f, 0xc8, 0x3e},
	}
	dxcoreNPUHardwareType = dxcoreGUID{
		data1: 0xd46140c4, data2: 0xadd7, data3: 0x451b,
		data4: [8]byte{0x9e, 0x56, 0x06, 0xfe, 0x8c, 0x3b, 0x58, 0xed},
	}
)

func currentNPUUsage() (int, bool) {
	adapterLUIDs, err := windowsNPUAdapterLUIDs()
	if err != nil || len(adapterLUIDs) == 0 {
		return 0, false
	}
	utilization, available, err := readWindowsNPUUsage(adapterLUIDs)
	if err != nil {
		return 0, false
	}
	return utilization, available
}

func readWindowsNPUUsage(adapterLUIDs []string) (int, bool, error) {
	powershell, err := exec.LookPath("powershell.exe")
	if err != nil {
		powershell, err = exec.LookPath("pwsh.exe")
		if err != nil {
			return 0, false, err
		}
	}
	quotedLUIDs := make([]string, len(adapterLUIDs))
	for i, luid := range adapterLUIDs {
		if len(luid) != len("luid_0x00000000_0x00000000") ||
			!strings.HasPrefix(luid, "luid_0x") ||
			strings.ContainsAny(luid, `'"`+"\r\n") {
			return 0, false, fmt.Errorf("invalid DXCore adapter LUID %q", luid)
		}
		quotedLUIDs[i] = "'" + luid + "'"
	}
	script := fmt.Sprintf(windowsNPUCounterScript, strings.Join(quotedLUIDs, ","))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, powershell, "-NoProfile", "-NonInteractive", "-Command", script).Output()
	if err != nil {
		return 0, false, err
	}
	utilization, available := parseNPUUsage(string(output))
	if !available {
		return 0, false, fmt.Errorf("Windows returned no usable NPU engine utilization")
	}
	return utilization, true, nil
}

func windowsNPUAdapterLUIDs() ([]string, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ole32 := syscall.NewLazyDLL("ole32.dll")
	hr, _, _ := ole32.NewProc("CoInitializeEx").Call(0, 0)
	initialized := int32(hr) >= 0
	if !initialized && uint32(hr) != 0x80010106 {
		return nil, fmt.Errorf("COM initialization failed: HRESULT 0x%08x", uint32(hr))
	}
	if initialized {
		defer ole32.NewProc("CoUninitialize").Call()
	}

	dxcore, err := syscall.LoadDLL("dxcore.dll")
	if err != nil {
		return nil, err
	}
	defer dxcore.Release()
	createFactory, err := dxcore.FindProc("DXCoreCreateAdapterFactory")
	if err != nil {
		return nil, err
	}
	var factory uintptr
	hr, _, _ = createFactory.Call(
		uintptr(unsafe.Pointer(&iidDXCoreAdapterFactory)),
		uintptr(unsafe.Pointer(&factory)),
	)
	if int32(hr) < 0 || factory == 0 {
		return nil, fmt.Errorf("DXCore adapter factory creation failed: HRESULT 0x%08x", uint32(hr))
	}
	defer releaseDXCoreObject(factory)

	attribute := dxcoreNPUHardwareType
	var adapterList uintptr
	hr = callDXCoreMethod(factory, 3,
		1,
		uintptr(unsafe.Pointer(&attribute)),
		uintptr(unsafe.Pointer(&iidDXCoreAdapterList)),
		uintptr(unsafe.Pointer(&adapterList)),
	)
	if int32(hr) < 0 || adapterList == 0 {
		return nil, fmt.Errorf("DXCore NPU adapter enumeration failed: HRESULT 0x%08x", uint32(hr))
	}
	defer releaseDXCoreObject(adapterList)

	count := uint32(callDXCoreMethod(adapterList, 4))
	luids := make([]string, 0, count)
	for index := uint32(0); index < count; index++ {
		var adapter uintptr
		hr = callDXCoreMethod(adapterList, 3,
			uintptr(index),
			uintptr(unsafe.Pointer(&iidDXCoreAdapter)),
			uintptr(unsafe.Pointer(&adapter)),
		)
		if int32(hr) < 0 || adapter == 0 {
			continue
		}

		var luid dxcoreLUID
		hr = callDXCoreMethod(adapter, 6,
			0,
			unsafe.Sizeof(luid),
			uintptr(unsafe.Pointer(&luid)),
		)
		releaseDXCoreObject(adapter)
		if int32(hr) < 0 {
			continue
		}
		luids = append(luids, fmt.Sprintf("luid_0x%08x_0x%08x", uint32(luid.highPart), luid.lowPart))
	}
	return luids, nil
}

func callDXCoreMethod(object uintptr, index uintptr, args ...uintptr) uintptr {
	vtable := *(*uintptr)(unsafe.Pointer(object))
	method := *(*uintptr)(unsafe.Pointer(vtable + index*unsafe.Sizeof(uintptr(0))))
	callArgs := append([]uintptr{object}, args...)
	result, _, _ := syscall.SyscallN(method, callArgs...)
	return result
}

func releaseDXCoreObject(object uintptr) {
	if object != 0 {
		callDXCoreMethod(object, 2)
	}
}
