package shard

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

const (
	ggufTypeUint8   = 0
	ggufTypeInt8    = 1
	ggufTypeUint16  = 2
	ggufTypeInt16   = 3
	ggufTypeUint32  = 4
	ggufTypeInt32   = 5
	ggufTypeFloat32 = 6
	ggufTypeBool    = 7
	ggufTypeString  = 8
	ggufTypeArray   = 9
	ggufTypeUint64  = 10
	ggufTypeInt64   = 11
	ggufTypeFloat64 = 12
)

const (
	maxGGUFMetadataEntries = 1_000_000
	maxGGUFArrayItems      = 10_000_000
	maxGGUFStringBytes     = 16 << 20
)

type ggufValue struct {
	number      uint64
	signed      int64
	isSigned    bool
	text        string
	arrayLength uint64
}

// ParseGGUF reads GGUF header metadata only. Tensor descriptors and tensor
// payloads are not loaded. The model context length is used as the default
// requested context; callers may replace it before planning.
func ParseGGUF(reader io.Reader) (ModelProfile, error) {
	return ParseGGUFWithContext(reader, 0)
}

// ParseGGUFWithFileSize parses header metadata and uses fileSizeBytes when no
// file size is present in the metadata.
func ParseGGUFWithFileSize(reader io.Reader, fileSizeBytes uint64) (ModelProfile, error) {
	profile, err := ParseGGUF(reader)
	if err != nil {
		return ModelProfile{}, err
	}
	if profile.FileSizeBytes == 0 {
		profile.FileSizeBytes = fileSizeBytes
	}
	return profile, nil
}

// ParseGGUFWithContext parses header metadata and uses requestedContext when it
// is positive, otherwise retaining the context length declared by the model.
func ParseGGUFWithContext(reader io.Reader, requestedContext int) (ModelProfile, error) {
	if reader == nil {
		return ModelProfile{}, errors.New("GGUF reader must not be nil")
	}
	if requestedContext < 0 {
		return ModelProfile{}, errors.New("requested context must not be negative")
	}
	metadata, err := readGGUFMetadata(reader)
	if err != nil {
		return ModelProfile{}, err
	}

	architecture := stringValue(metadata, "general.architecture")
	if architecture == "" {
		return ModelProfile{}, errors.New("GGUF metadata is missing general.architecture")
	}
	prefix := architecture + "."
	hiddenSize := intValue(metadata, prefix+"embedding_length")
	layerCount := intValue(metadata, prefix+"block_count")
	attentionHeads := intValue(metadata, prefix+"attention.head_count")
	kvHeads := intValue(metadata, prefix+"attention.head_count_kv")
	if kvHeads == 0 {
		kvHeads = attentionHeads
	}
	headDim := intValue(metadata, prefix+"attention.key_length")
	if headDim == 0 {
		headDim = intValue(metadata, prefix+"attention.value_length")
	}
	if headDim == 0 && hiddenSize > 0 && attentionHeads > 0 && hiddenSize%attentionHeads == 0 {
		headDim = hiddenSize / attentionHeads
	}
	vocabSize := intValue(metadata, prefix+"vocab_size")
	if vocabSize == 0 {
		vocabSize = intValue(metadata, "tokenizer.ggml.tokens")
	}
	contextLength := intValue(metadata, prefix+"context_length")
	if requestedContext > 0 {
		contextLength = requestedContext
	}

	quantization := stringValue(metadata, "general.quantization")
	if quantization == "" {
		if fileType, ok := unsignedValue(metadata, "general.file_type"); ok {
			quantization = ggufQuantization(fileType)
		}
	}
	profile := ModelProfile{
		Architecture:     architecture,
		Parameters:       valueOrZero(metadata, "general.parameter_count"),
		LayerCount:       layerCount,
		HiddenSize:       hiddenSize,
		KVHeads:          kvHeads,
		HeadDim:          headDim,
		VocabSize:        vocabSize,
		Quantization:     quantization,
		FileSizeBytes:    valueOrZero(metadata, "general.file_size"),
		WeightsSizeBytes: valueOrZero(metadata, "general.weights_size"),
		RequestedContext: contextLength,
	}
	if profile.LayerCount <= 0 || profile.HiddenSize <= 0 || profile.KVHeads <= 0 || profile.HeadDim <= 0 {
		return profile, errors.New("GGUF metadata is missing valid layer, embedding, or attention dimensions")
	}
	return profile, nil
}

func readGGUFMetadata(reader io.Reader) (map[string]ggufValue, error) {
	var magic [4]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return nil, fmt.Errorf("read GGUF magic: %w", err)
	}
	if string(magic[:]) != "GGUF" {
		return nil, fmt.Errorf("invalid GGUF magic %q", magic)
	}
	version, err := readUint32(reader)
	if err != nil {
		return nil, fmt.Errorf("read GGUF version: %w", err)
	}
	if version < 2 || version > 3 {
		return nil, fmt.Errorf("unsupported GGUF version %d", version)
	}
	if _, err := readUint64(reader); err != nil {
		return nil, fmt.Errorf("read GGUF tensor count: %w", err)
	}
	metadataCount, err := readUint64(reader)
	if err != nil {
		return nil, fmt.Errorf("read GGUF metadata count: %w", err)
	}
	if metadataCount > maxGGUFMetadataEntries {
		return nil, fmt.Errorf("GGUF metadata count %d exceeds limit %d", metadataCount, maxGGUFMetadataEntries)
	}

	metadata := make(map[string]ggufValue, int(metadataCount))
	for i := uint64(0); i < metadataCount; i++ {
		key, err := readGGUFString(reader)
		if err != nil {
			return nil, fmt.Errorf("read GGUF metadata key %d: %w", i, err)
		}
		valueType, err := readUint32(reader)
		if err != nil {
			return nil, fmt.Errorf("read GGUF metadata type for %q: %w", key, err)
		}
		value, err := readGGUFValue(reader, valueType)
		if err != nil {
			return nil, fmt.Errorf("read GGUF metadata value %q: %w", key, err)
		}
		metadata[key] = value
	}
	return metadata, nil
}

func readGGUFValue(reader io.Reader, valueType uint32) (ggufValue, error) {
	var value ggufValue
	switch valueType {
	case ggufTypeUint8:
		var raw [1]byte
		_, err := io.ReadFull(reader, raw[:])
		value.number = uint64(raw[0])
		return value, err
	case ggufTypeInt8:
		var raw [1]byte
		_, err := io.ReadFull(reader, raw[:])
		value.signed, value.isSigned = int64(int8(raw[0])), true
		return value, err
	case ggufTypeUint16:
		number, err := readUint16(reader)
		value.number = uint64(number)
		return value, err
	case ggufTypeInt16:
		number, err := readUint16(reader)
		value.signed, value.isSigned = int64(int16(number)), true
		return value, err
	case ggufTypeUint32:
		number, err := readUint32(reader)
		value.number = uint64(number)
		return value, err
	case ggufTypeInt32:
		number, err := readUint32(reader)
		value.signed, value.isSigned = int64(int32(number)), true
		return value, err
	case ggufTypeUint64:
		var err error
		value.number, err = readUint64(reader)
		return value, err
	case ggufTypeInt64:
		number, err := readUint64(reader)
		value.signed, value.isSigned = int64(number), true
		return value, err
	case ggufTypeFloat32:
		return value, discardBytes(reader, 4)
	case ggufTypeFloat64:
		return value, discardBytes(reader, 8)
	case ggufTypeBool:
		var raw [1]byte
		_, err := io.ReadFull(reader, raw[:])
		value.number = uint64(raw[0])
		return value, err
	case ggufTypeString:
		text, err := readGGUFString(reader)
		value.text = text
		return value, err
	case ggufTypeArray:
		elementType, err := readUint32(reader)
		if err != nil {
			return value, err
		}
		count, err := readUint64(reader)
		if err != nil {
			return value, err
		}
		if count > maxGGUFArrayItems {
			return value, fmt.Errorf("array length %d exceeds limit %d", count, maxGGUFArrayItems)
		}
		if elementType == ggufTypeArray {
			return value, errors.New("nested GGUF arrays are unsupported")
		}
		if err := skipGGUFArray(reader, elementType, count); err != nil {
			return value, err
		}
		value.arrayLength = count
		value.number = count
		return value, nil
	default:
		return value, fmt.Errorf("unsupported GGUF value type %d", valueType)
	}
}

func skipGGUFArray(reader io.Reader, elementType uint32, count uint64) error {
	if elementType == ggufTypeString {
		for i := uint64(0); i < count; i++ {
			if err := skipGGUFString(reader); err != nil {
				return err
			}
		}
		return nil
	}
	size, ok := ggufTypeSize(elementType)
	if !ok {
		return fmt.Errorf("unsupported GGUF array element type %d", elementType)
	}
	if count != 0 && count > math.MaxInt64/uint64(size) {
		return errors.New("GGUF array byte length overflows")
	}
	return discardBytes(reader, int64(count*uint64(size)))
}

func ggufTypeSize(valueType uint32) (uint64, bool) {
	switch valueType {
	case ggufTypeUint8, ggufTypeInt8, ggufTypeBool:
		return 1, true
	case ggufTypeUint16, ggufTypeInt16:
		return 2, true
	case ggufTypeUint32, ggufTypeInt32, ggufTypeFloat32:
		return 4, true
	case ggufTypeUint64, ggufTypeInt64, ggufTypeFloat64:
		return 8, true
	default:
		return 0, false
	}
}

func readGGUFString(reader io.Reader) (string, error) {
	length, err := readUint64(reader)
	if err != nil {
		return "", err
	}
	if length > maxGGUFStringBytes {
		return "", fmt.Errorf("string length %d exceeds limit %d", length, maxGGUFStringBytes)
	}
	raw := make([]byte, int(length))
	if _, err := io.ReadFull(reader, raw); err != nil {
		return "", err
	}
	return string(raw), nil
}

func skipGGUFString(reader io.Reader) error {
	length, err := readUint64(reader)
	if err != nil {
		return err
	}
	if length > maxGGUFStringBytes {
		return fmt.Errorf("string length %d exceeds limit %d", length, maxGGUFStringBytes)
	}
	return discardBytes(reader, int64(length))
}

func discardBytes(reader io.Reader, length int64) error {
	_, err := io.CopyN(io.Discard, reader, length)
	return err
}

func readUint16(reader io.Reader) (uint16, error) {
	var raw [2]byte
	_, err := io.ReadFull(reader, raw[:])
	return binary.LittleEndian.Uint16(raw[:]), err
}

func readUint32(reader io.Reader) (uint32, error) {
	var raw [4]byte
	_, err := io.ReadFull(reader, raw[:])
	return binary.LittleEndian.Uint32(raw[:]), err
}

func readUint64(reader io.Reader) (uint64, error) {
	var raw [8]byte
	_, err := io.ReadFull(reader, raw[:])
	return binary.LittleEndian.Uint64(raw[:]), err
}

func stringValue(metadata map[string]ggufValue, key string) string {
	return strings.TrimSpace(metadata[key].text)
}

func unsignedValue(metadata map[string]ggufValue, key string) (uint64, bool) {
	value, ok := metadata[key]
	if !ok {
		return 0, false
	}
	if value.isSigned {
		if value.signed < 0 {
			return 0, false
		}
		return uint64(value.signed), true
	}
	return value.number, true
}

func valueOrZero(metadata map[string]ggufValue, key string) uint64 {
	value, _ := unsignedValue(metadata, key)
	return value
}

func intValue(metadata map[string]ggufValue, key string) int {
	value, ok := unsignedValue(metadata, key)
	if !ok || value > uint64(math.MaxInt) {
		return 0
	}
	return int(value)
}

func ggufQuantization(fileType uint64) string {
	quantizations := [...]string{
		"F32", "F16", "Q4_0", "Q4_1", "Q4_1", "Q4_2", "Q4_3", "Q8_0",
		"Q5_0", "Q5_1", "Q2_K", "Q3_K_S", "Q3_K_M", "Q3_K_L", "Q4_K_S",
		"Q4_K_M", "Q5_K_S", "Q5_K_M", "Q6_K", "IQ2_XXS", "IQ2_XS", "IQ3_XXS",
		"IQ1_S", "IQ4_NL", "IQ3_S", "IQ3_M", "IQ2_S", "IQ4_XS", "IQ1_M", "BF16",
	}
	if fileType < uint64(len(quantizations)) {
		return quantizations[fileType]
	}
	return fmt.Sprintf("file-type-%d", fileType)
}
