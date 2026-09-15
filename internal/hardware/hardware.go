// Package hardware detects local machine capabilities used to score a node
// for LLM engine/model suitability (CPU, RAM, GPU presence).
package hardware

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// Info describes the detected capabilities of the host machine.
type Info struct {
	OS         string  `json:"os"`
	Arch       string  `json:"arch"`
	CPUCores   int     `json:"cpu_cores"`
	TotalRAMMB uint64  `json:"total_ram_mb"`
	HasGPU     bool    `json:"has_gpu"`
	GPUVendor  string  `json:"gpu_vendor,omitempty"`
	HasNPU     bool    `json:"has_npu"`
	NPUVendor  string  `json:"npu_vendor,omitempty"`
	Score      float64 `json:"score"`
}

// Detect gathers hardware information about the current host.
func Detect() Info {
	info := Info{
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		CPUCores: runtime.NumCPU(),
	}
	info.TotalRAMMB = totalMemoryMB()
	info.HasGPU, info.GPUVendor = detectGPU()
	info.HasNPU, info.NPUVendor = detectNPU()
	info.Score = computeScore(info)
	return info
}

// computeScore produces a single relative capability score used by the
// scheduler to rank nodes when hardware is the deciding factor.
func computeScore(info Info) float64 {
	score := float64(info.CPUCores) * 1.0
	score += float64(info.TotalRAMMB) / 1024.0 * 0.5 // reward per GB of RAM
	if info.HasGPU {
		score += 25 // large boost, GPU-accelerated inference is far faster
	}
	if info.HasNPU {
		score += 30 // prefer nodes with a neural accelerator for compatible servers
	}
	return score
}

// detectNPU makes a best-effort, dependency-free probe for neural accelerators.
// It intentionally reports capability only; a compatible inference server still
// has to be running locally before the node advertises its models.
func detectNPU() (bool, string) {
	switch runtime.GOOS {
	case "darwin":
		if runtime.GOARCH == "arm64" {
			return true, "apple-neural-engine"
		}
	case "windows":
		if _, err := exec.LookPath("powershell.exe"); err == nil {
			out, err := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", "Get-PnpDevice -PresentOnly | Select-Object -ExpandProperty FriendlyName").Output()
			if err == nil {
				if vendor := npuVendor(string(out)); vendor != "" {
					return true, vendor
				}
			}
		}
	case "linux":
		for _, path := range []string{"/dev/accel", "/dev/ivpu", "/dev/nnapi"} {
			if _, err := os.Stat(path); err == nil {
				return true, "linux-npu"
			}
		}
		if _, err := exec.LookPath("lspci"); err == nil {
			out, err := exec.Command("lspci").Output()
			if err == nil {
				if vendor := npuVendor(string(out)); vendor != "" {
					return true, vendor
				}
			}
		}
	}
	return false, ""
}

func npuVendor(text string) string {
	value := strings.ToLower(text)
	for _, match := range []struct {
		needle string
		vendor string
	}{
		{"neural engine", "apple-neural-engine"},
		{"ai boost", "intel-npu"},
		{"gaudi", " habana"},
		{"ipu", "intel-npu"},
		{"hexagon", "qualcomm-npu"},
		{"hiai", "huawei-npu"},
		{"hailo", "hailo-npu"},
		{"npu", "npu"},
	} {
		if strings.Contains(value, match.needle) {
			return strings.TrimSpace(match.vendor)
		}
	}
	return ""
}

// detectGPU makes a best-effort, dependency-free attempt to detect a GPU
// suitable for LLM acceleration by probing for common vendor CLI tools.
func detectGPU() (bool, string) {
	if _, err := exec.LookPath("nvidia-smi"); err == nil {
		return true, "nvidia"
	}
	if _, err := exec.LookPath("rocm-smi"); err == nil {
		return true, "amd"
	}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		// Apple Silicon has a unified-memory GPU usable via Metal.
		return true, "apple"
	}
	return false, ""
}
