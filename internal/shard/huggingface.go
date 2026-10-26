package shard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

const maxHuggingFaceConfigBytes = 16 << 20

// ParseHuggingFaceConfig parses architecture and size metadata from config.json.
// max_position_embeddings is used as the default requested context when present.
func ParseHuggingFaceConfig(reader io.Reader) (ModelProfile, error) {
	return ParseHuggingFaceConfigWithContext(reader, 0)
}

// ParseHuggingFaceConfigWithContext parses config.json and optionally overrides
// the context from the file with requestedContext.
func ParseHuggingFaceConfigWithContext(reader io.Reader, requestedContext int) (ModelProfile, error) {
	if reader == nil {
		return ModelProfile{}, errors.New("model config reader for Hugging Face must not be nil")
	}
	if requestedContext < 0 {
		return ModelProfile{}, errors.New("requested context must not be negative")
	}
	rawConfig, err := io.ReadAll(io.LimitReader(reader, maxHuggingFaceConfigBytes+1))
	if err != nil {
		return ModelProfile{}, fmt.Errorf("read Hugging Face config: %w", err)
	}
	if len(rawConfig) > maxHuggingFaceConfigBytes {
		return ModelProfile{}, fmt.Errorf("model config from Hugging Face exceeds %d bytes", maxHuggingFaceConfigBytes)
	}

	decoder := json.NewDecoder(bytes.NewReader(rawConfig))
	var config map[string]json.RawMessage
	if err := decoder.Decode(&config); err != nil {
		return ModelProfile{}, fmt.Errorf("decode Hugging Face config: %w", err)
	}
	if config == nil {
		return ModelProfile{}, errors.New("model config from Hugging Face must be a JSON object")
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		if err == nil {
			return ModelProfile{}, errors.New("model config from Hugging Face contains more than one JSON value")
		}
		return ModelProfile{}, fmt.Errorf("decode trailing Hugging Face config data: %w", err)
	}

	hiddenSize := intFromJSON(config, "hidden_size", "d_model", "n_embd")
	layerCount := intFromJSON(config, "num_hidden_layers", "n_layer", "num_layers")
	attentionHeads := intFromJSON(config, "num_attention_heads", "n_head", "num_heads")
	kvHeads := intFromJSON(config, "num_key_value_heads", "num_kv_heads")
	if kvHeads == 0 {
		kvHeads = attentionHeads
	}
	headDim := intFromJSON(config, "head_dim")
	if headDim == 0 && hiddenSize > 0 && attentionHeads > 0 && hiddenSize%attentionHeads == 0 {
		headDim = hiddenSize / attentionHeads
	}
	contextLength := requestedContext
	if contextLength == 0 {
		contextLength = intFromJSON(config, "max_position_embeddings", "n_positions", "max_seq_len")
	}

	architecture := stringFromJSON(config, "model_type")
	if architecture == "" {
		architecture = firstArrayString(config["architectures"])
	}
	quantization := huggingFaceQuantization(config)

	profile := ModelProfile{
		Architecture:     architecture,
		Parameters:       uintFromJSON(config, "num_parameters", "n_parameters", "parameter_count", "num_params"),
		LayerCount:       layerCount,
		HiddenSize:       hiddenSize,
		KVHeads:          kvHeads,
		HeadDim:          headDim,
		VocabSize:        intFromJSON(config, "vocab_size"),
		Quantization:     quantization,
		FileSizeBytes:    uintFromJSON(config, "file_size_bytes", "weights_size_bytes", "total_size"),
		WeightsSizeBytes: uintFromJSON(config, "weight_size_bytes", "weights_size_bytes"),
		RequestedContext: contextLength,
	}
	if profile.LayerCount <= 0 || profile.HiddenSize <= 0 || profile.KVHeads <= 0 || profile.HeadDim <= 0 {
		return profile, errors.New("model config from Hugging Face is missing valid layer, hidden-size, or attention-head metadata")
	}
	return profile, nil
}

func intFromJSON(values map[string]json.RawMessage, keys ...string) int {
	value := uintFromJSON(values, keys...)
	if value == 0 || value > uint64(math.MaxInt) {
		return 0
	}
	return int(value)
}

func uintFromJSON(values map[string]json.RawMessage, keys ...string) uint64 {
	for _, key := range keys {
		raw, ok := values[key]
		if !ok {
			continue
		}
		value := strings.TrimSpace(string(raw))
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			value = value[1 : len(value)-1]
		}
		number, err := strconv.ParseUint(value, 10, 64)
		if err == nil {
			return number
		}
	}
	return 0
}

func stringFromJSON(values map[string]json.RawMessage, key string) string {
	var value string
	if err := json.Unmarshal(values[key], &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func firstArrayString(raw json.RawMessage) string {
	var values []string
	if err := json.Unmarshal(raw, &values); err != nil || len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func huggingFaceQuantization(config map[string]json.RawMessage) string {
	var fallback string
	for _, key := range []string{"quantization", "quantization_method"} {
		if value := stringFromJSON(config, key); value != "" {
			if _, ok := quantizationBits(value); ok {
				return value
			}
			fallback = value
		}
	}
	var quantization map[string]json.RawMessage
	if err := json.Unmarshal(config["quantization_config"], &quantization); err == nil {
		if bits := intFromJSON(quantization, "bits", "wbits"); bits > 0 {
			return fmt.Sprintf("Q%d", bits)
		}
		if quantization["load_in_4bit"] != nil && string(quantization["load_in_4bit"]) == "true" {
			return "Q4"
		}
		if quantization["load_in_8bit"] != nil && string(quantization["load_in_8bit"]) == "true" {
			return "Q8"
		}
		if value := stringFromJSON(quantization, "bnb_4bit_quant_type"); value != "" {
			return "Q4_" + strings.ToUpper(value)
		}
		if value := stringFromJSON(quantization, "quant_method"); value != "" {
			fallback = value
		}
	}
	dtype := strings.ToLower(stringFromJSON(config, "torch_dtype"))
	switch dtype {
	case "float32":
		return "F32"
	case "float16":
		return "F16"
	case "bfloat16":
		return "BF16"
	}
	return fallback
}
