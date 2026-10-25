package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

func TestParseGGUFReadsMetadataWithoutReadingTensorData(t *testing.T) {
	metadata := []ggufTestEntry{
		ggufStringEntry("general.architecture", "llama"),
		ggufUintEntry("general.file_type", ggufTypeUint32, 15),
		ggufUintEntry("general.parameter_count", ggufTypeUint64, 7_000_000_000),
		ggufUintEntry("general.weights_size", ggufTypeUint64, 4_000_000_000),
		ggufUintEntry("llama.block_count", ggufTypeUint32, 32),
		ggufUintEntry("llama.embedding_length", ggufTypeUint32, 4096),
		ggufUintEntry("llama.attention.head_count", ggufTypeUint32, 32),
		ggufUintEntry("llama.attention.head_count_kv", ggufTypeUint32, 8),
		ggufUintEntry("llama.context_length", ggufTypeUint32, 8192),
		ggufArrayEntry("tokenizer.ggml.tokens", ggufTypeUint32, 32000),
	}
	raw := makeGGUFHeader(t, metadata)
	raw = append(raw, []byte("TENSOR_DATA_SENTINEL")...)
	reader := bytes.NewReader(raw)

	profile, err := ParseGGUF(reader)
	if err != nil {
		t.Fatalf("ParseGGUF() error = %v", err)
	}
	if profile.Architecture != "llama" || profile.Parameters != 7_000_000_000 ||
		profile.LayerCount != 32 || profile.HiddenSize != 4096 ||
		profile.KVHeads != 8 || profile.HeadDim != 128 || profile.VocabSize != 32000 ||
		profile.Quantization != "Q4_K_M" || profile.WeightsSizeBytes != 4_000_000_000 ||
		profile.RequestedContext != 8192 {
		t.Fatalf("ParseGGUF() profile = %+v", profile)
	}
	if remaining := reader.Len(); remaining != len("TENSOR_DATA_SENTINEL") {
		t.Fatalf("ParseGGUF() consumed tensor data; %d bytes remain", remaining)
	}
}

func TestParseGGUFContextOverrideAndMalformedHeaders(t *testing.T) {
	entries := []ggufTestEntry{
		ggufStringEntry("general.architecture", "llama"),
		ggufUintEntry("llama.block_count", ggufTypeUint32, 32),
		ggufUintEntry("llama.embedding_length", ggufTypeUint32, 4096),
		ggufUintEntry("llama.attention.head_count", ggufTypeUint32, 32),
		ggufUintEntry("llama.context_length", ggufTypeUint32, 8192),
	}
	header := makeGGUFHeader(t, entries)
	profile, err := ParseGGUFWithContext(bytes.NewReader(header), 2048)
	if err != nil {
		t.Fatalf("ParseGGUFWithContext() error = %v", err)
	}
	if profile.RequestedContext != 2048 || profile.KVHeads != 32 || profile.HeadDim != 128 {
		t.Fatalf("ParseGGUFWithContext() profile = %+v", profile)
	}
	profile, err = ParseGGUFWithFileSize(bytes.NewReader(header), 7_000_000_000)
	if err != nil {
		t.Fatalf("ParseGGUFWithFileSize() error = %v", err)
	}
	if profile.FileSizeBytes != 7_000_000_000 {
		t.Fatalf("file size = %d, want 7000000000", profile.FileSizeBytes)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "bad magic", data: []byte("NOPE")},
		{name: "unsupported version", data: []byte("GGUF\x01\x00\x00\x00")},
		{name: "missing dimensions", data: makeGGUFHeader(t, []ggufTestEntry{
			ggufStringEntry("general.architecture", "llama"),
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseGGUF(bytes.NewReader(test.data)); err == nil {
				t.Fatal("ParseGGUF() error = nil, want error")
			}
		})
	}
}

func TestParseHuggingFaceConfig(t *testing.T) {
	config := `{
		"model_type":"llama",
		"architectures":["LlamaForCausalLM"],
		"hidden_size":4096,
		"num_hidden_layers":32,
		"num_attention_heads":32,
		"num_key_value_heads":8,
		"vocab_size":32000,
		"max_position_embeddings":8192,
		"num_parameters":7000000000,
		"weights_size_bytes":4000000000,
		"quantization_config":{"bits":4}
	}`
	profile, err := ParseHuggingFaceConfig(strings.NewReader(config))
	if err != nil {
		t.Fatalf("ParseHuggingFaceConfig() error = %v", err)
	}
	if profile.Architecture != "llama" || profile.Parameters != 7_000_000_000 ||
		profile.LayerCount != 32 || profile.HiddenSize != 4096 ||
		profile.KVHeads != 8 || profile.HeadDim != 128 || profile.VocabSize != 32000 ||
		profile.Quantization != "Q4" || profile.WeightsSizeBytes != 4_000_000_000 ||
		profile.RequestedContext != 8192 {
		t.Fatalf("ParseHuggingFaceConfig() profile = %+v", profile)
	}
	size, err := profile.EffectiveWeightsSizeBytes()
	if err != nil || size != 4_000_000_000 {
		t.Fatalf("EffectiveWeightsSizeBytes() = %d, %v", size, err)
	}

	override, err := ParseHuggingFaceConfigWithContext(strings.NewReader(config), 4096)
	if err != nil {
		t.Fatalf("ParseHuggingFaceConfigWithContext() error = %v", err)
	}
	if override.RequestedContext != 4096 {
		t.Fatalf("requested context = %d, want 4096", override.RequestedContext)
	}
	withoutFileSize := strings.Replace(config, `"weights_size_bytes":4000000000,`, "", 1)
	estimated, err := ParseHuggingFaceConfig(strings.NewReader(withoutFileSize))
	if err != nil {
		t.Fatalf("ParseHuggingFaceConfig() without explicit weight size error = %v", err)
	}
	if size, err := estimated.EffectiveWeightsSizeBytes(); err != nil || size != 3_500_000_000 {
		t.Fatalf("estimated HF weights = %d, %v; want 3500000000, nil", size, err)
	}
}

func TestEffectiveWeightsSizeEstimate(t *testing.T) {
	profile := ModelProfile{Parameters: 101, Quantization: "Q4_K_M"}
	if size, err := profile.EffectiveWeightsSizeBytes(); err != nil || size != 51 {
		t.Fatalf("EffectiveWeightsSizeBytes() = %d, %v; want 51, nil", size, err)
	}
	if _, err := (ModelProfile{Parameters: 10}).EffectiveWeightsSizeBytes(); err == nil {
		t.Fatal("unknown quantization should not produce an estimate")
	}
}

type ggufTestEntry struct {
	key       string
	valueType uint32
	value     []byte
}

func ggufStringEntry(key, value string) ggufTestEntry {
	var encoded bytes.Buffer
	writeGGUFTestString(&encoded, value)
	return ggufTestEntry{key: key, valueType: ggufTypeString, value: encoded.Bytes()}
}

func ggufUintEntry(key string, valueType uint32, value uint64) ggufTestEntry {
	var encoded bytes.Buffer
	switch valueType {
	case ggufTypeUint32:
		_ = binary.Write(&encoded, binary.LittleEndian, uint32(value))
	case ggufTypeUint64:
		_ = binary.Write(&encoded, binary.LittleEndian, value)
	default:
		panic(fmt.Sprintf("unsupported test GGUF integer type %d", valueType))
	}
	return ggufTestEntry{key: key, valueType: valueType, value: encoded.Bytes()}
}

func ggufArrayEntry(key string, elementType uint32, count uint64) ggufTestEntry {
	var encoded bytes.Buffer
	_ = binary.Write(&encoded, binary.LittleEndian, elementType)
	_ = binary.Write(&encoded, binary.LittleEndian, count)
	for i := uint64(0); i < count; i++ {
		_ = binary.Write(&encoded, binary.LittleEndian, uint32(0))
	}
	return ggufTestEntry{key: key, valueType: ggufTypeArray, value: encoded.Bytes()}
}

func makeGGUFHeader(t *testing.T, metadata []ggufTestEntry) []byte {
	t.Helper()
	var encoded bytes.Buffer
	encoded.WriteString("GGUF")
	_ = binary.Write(&encoded, binary.LittleEndian, uint32(3))
	_ = binary.Write(&encoded, binary.LittleEndian, uint64(64))
	_ = binary.Write(&encoded, binary.LittleEndian, uint64(len(metadata)))
	for _, entry := range metadata {
		writeGGUFTestString(&encoded, entry.key)
		_ = binary.Write(&encoded, binary.LittleEndian, entry.valueType)
		_, _ = encoded.Write(entry.value)
	}
	return encoded.Bytes()
}

func writeGGUFTestString(writer *bytes.Buffer, value string) {
	_ = binary.Write(writer, binary.LittleEndian, uint64(len(value)))
	writer.WriteString(value)
}
