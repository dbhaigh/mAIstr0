// Package hardware detects local machine capabilities used to score a node
// for LLM engine/model suitability (CPU, RAM, GPU presence).
package hardware

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// GPUUsageSample is one five-second GPU telemetry snapshot. Utilization is
// aggregated across detected NVIDIA GPUs and is only meaningful when Available.
type GPUUsageSample struct {
	Timestamp   time.Time `json:"timestamp"`
	Available   bool      `json:"available"`
	Utilization int       `json:"utilization_percent"`
}

// SystemUsageSample is a five-second sample of host CPU and memory pressure.
type SystemUsageSample struct {
	Timestamp         time.Time `json:"timestamp"`
	CPUAvailable      bool      `json:"cpu_available"`
	CPUUtilization    int       `json:"cpu_utilization_percent"`
	MemoryAvailable   bool      `json:"memory_available"`
	MemoryUtilization int       `json:"memory_usage_percent"`
}

// Info describes the detected capabilities of the host machine.
type Info struct {
	OS                   string  `json:"os"`
	Arch                 string  `json:"arch"`
	CPUCores             int     `json:"cpu_cores"`
	CPUStatsAvailable    bool    `json:"cpu_stats_available"`
	CPUUtilization       int     `json:"cpu_utilization_percent,omitempty"`
	TotalRAMMB           uint64  `json:"total_ram_mb"`
	MemoryStatsAvailable bool    `json:"memory_stats_available"`
	MemoryUsage          int     `json:"memory_usage_percent,omitempty"`
	HasGPU               bool    `json:"has_gpu"`
	GPUVendor            string  `json:"gpu_vendor,omitempty"`
	GPUStatsAvailable    bool    `json:"gpu_stats_available"`
	GPUUtilization       int     `json:"gpu_utilization_percent,omitempty"`
	GPUMemoryFreeMB      uint64  `json:"gpu_memory_free_mb,omitempty"`
	HasNPU               bool    `json:"has_npu"`
	NPUVendor            string  `json:"npu_vendor,omitempty"`
	Score                float64 `json:"score"`
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

// CurrentGPUUsage returns current NVIDIA GPU utilization and free memory when
// nvidia-smi is available. Other GPU vendors remain unknown rather than being
// reported as idle.
func CurrentGPUUsage() (utilization int, freeMemoryMB uint64, available bool) {
	if _, err := exec.LookPath("nvidia-smi"); err != nil {
		return 0, 0, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=utilization.gpu,memory.free", "--format=csv,noheader,nounits").Output()
	if err != nil {
		return 0, 0, false
	}
	return parseNVIDIAUsage(string(out))
}

// CurrentSystemUsage returns host CPU and memory utilization where the
// operating system exposes reliable counters.
func CurrentSystemUsage() (cpuUtilization int, cpuAvailable bool, memoryUtilization int, memoryAvailable bool) {
	return currentSystemUsage()
}

func percentUsed(total, available uint64) (int, bool) {
	if total == 0 || available > total {
		return 0, false
	}
	return int((total - available) * 100 / total), true
}

func parseNVIDIAUsage(output string) (int, uint64, bool) {
	maxUtilization := 0
	maxFreeMemory := uint64(0)
	found := false
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.Split(line, ",")
		if len(fields) != 2 {
			continue
		}
		util, err := strconv.Atoi(strings.TrimSpace(fields[0]))
		if err != nil || util < 0 || util > 100 {
			continue
		}
		free, err := strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
		if err != nil {
			continue
		}
		if !found || util > maxUtilization {
			maxUtilization = util
		}
		if !found || free > maxFreeMemory {
			maxFreeMemory = free
		}
		found = true
	}
	return maxUtilization, maxFreeMemory, found
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
