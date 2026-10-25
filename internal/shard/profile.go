package shard

import (
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// ModelProfile contains the metadata needed to estimate a model's sharded
// inference memory without loading its tensors.
type ModelProfile struct {
	Name             string `json:"name,omitempty"`
	Architecture     string `json:"architecture,omitempty"`
	Parameters       uint64 `json:"parameters,omitempty"`
	LayerCount       int    `json:"layer_count"`
	HiddenSize       int    `json:"hidden_size"`
	KVHeads          int    `json:"kv_heads"`
	HeadDim          int    `json:"head_dim"`
	VocabSize        int    `json:"vocab_size,omitempty"`
	Quantization     string `json:"quantization,omitempty"`
	FileSizeBytes    uint64 `json:"file_size_bytes,omitempty"`
	WeightsSizeBytes uint64 `json:"weights_size_bytes,omitempty"`
	RequestedContext int    `json:"requested_context"`
}

var errUnknownWeightsSize = errors.New("model weight size cannot be estimated from the supplied profile")

// EffectiveWeightsSizeBytes returns the supplied weight/file size, or estimates
// it from parameter count and quantization when an explicit size is unavailable.
func (model ModelProfile) EffectiveWeightsSizeBytes() (uint64, error) {
	if model.WeightsSizeBytes > 0 {
		return model.WeightsSizeBytes, nil
	}
	if model.FileSizeBytes > 0 {
		return model.FileSizeBytes, nil
	}
	if model.Parameters == 0 {
		return 0, errUnknownWeightsSize
	}
	bitsPerWeight, ok := quantizationBits(model.Quantization)
	if !ok {
		return 0, fmt.Errorf("%w: unknown quantization %q", errUnknownWeightsSize, model.Quantization)
	}
	return multiplyDivideCeil(model.Parameters, uint64(bitsPerWeight), 8)
}

func validateModelProfile(model ModelProfile) error {
	var problems []string
	if model.LayerCount <= 0 {
		problems = append(problems, "layer count must be greater than zero")
	}
	if model.HiddenSize <= 0 {
		problems = append(problems, "hidden size must be greater than zero")
	}
	if model.KVHeads <= 0 {
		problems = append(problems, "KV head count must be greater than zero")
	}
	if model.HeadDim <= 0 {
		problems = append(problems, "head dimension must be greater than zero")
	}
	if model.RequestedContext <= 0 {
		problems = append(problems, "requested context must be greater than zero")
	}
	if _, err := model.EffectiveWeightsSizeBytes(); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func quantizationBits(value string) (int, bool) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "f32", "float32", "fp32":
		return 32, true
	case "f16", "float16", "fp16", "bf16", "bfloat16":
		return 16, true
	case "q8", "q8_0", "q8_1", "int8", "8bit", "8-bit":
		return 8, true
	}
	start := strings.IndexByte(normalized, 'q')
	if start < 0 {
		return 0, false
	}
	digit := start + 1
	end := digit
	for end < len(normalized) && normalized[end] >= '0' && normalized[end] <= '9' {
		end++
	}
	if end == digit {
		return 0, false
	}
	bitsPerWeight, err := strconv.Atoi(normalized[digit:end])
	return bitsPerWeight, err == nil && bitsPerWeight > 0 && bitsPerWeight <= 32
}

func multiplyDivideCeil(a, b, divisor uint64) (uint64, error) {
	if divisor == 0 {
		return 0, errors.New("division by zero")
	}
	hi, lo := bits.Mul64(a, b)
	if hi >= divisor {
		return 0, errors.New("model size calculation overflows uint64")
	}
	quotient, remainder := bits.Div64(hi, lo, divisor)
	if remainder != 0 {
		if quotient == ^uint64(0) {
			return 0, errors.New("model size calculation overflows uint64")
		}
		quotient++
	}
	return quotient, nil
}
