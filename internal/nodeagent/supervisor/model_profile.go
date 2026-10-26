package supervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/shard"
)

const maxProfileModelFiles = 4096

// ModelProfile reads metadata only from a model path configured for this engine.
func (m *Manager) ModelProfile(id string, requestedContext int) (shard.ModelProfile, error) {
	if requestedContext < 0 {
		return shard.ModelProfile{}, errors.New("requested context must not be negative")
	}
	engine, err := m.engine(id)
	if err != nil {
		return shard.ModelProfile{}, err
	}
	configured := engine.config
	if configured.Kind == config.ManagedEngineGGMLRPCServer {
		return shard.ModelProfile{}, errors.New("RPC worker engines do not have a model profile")
	}
	if err := config.ValidateManagedEngines(
		[]config.ManagedEngineConfig{configured},
		engine.binaryAllowlist,
		engine.modelAllowlist,
	); err != nil {
		return shard.ModelProfile{}, fmt.Errorf("revalidate managed model path: %w", err)
	}
	var profile shard.ModelProfile
	switch {
	case engine.modelName != "" && configured.Kind == config.ManagedEngineLlamaServer:
		if _, err := approvedModelRoot(configured.ModelPath, engine.modelAllowlist); err != nil {
			return profile, err
		}
		profile, err = readLocalGGUF(configured.ModelPath, requestedContext)
	case configured.ModelPath != "":
		info, statErr := os.Stat(configured.ModelPath)
		if statErr != nil {
			return shard.ModelProfile{}, fmt.Errorf("stat configured model path: %w", statErr)
		}
		if configured.Kind == config.ManagedEngineLlamaServer &&
			(info.IsDir() || !strings.EqualFold(filepath.Ext(configured.ModelPath), ".gguf")) {
			return shard.ModelProfile{}, errors.New("llama-server model_path must be a GGUF file")
		}
		if configured.Kind == config.ManagedEngineVLLM && !info.IsDir() {
			return shard.ModelProfile{}, errors.New("vLLM model_path must be a Hugging Face model directory")
		}
		profile, err = readConfiguredModelPath(configured.ModelPath, engine.modelAllowlist, requestedContext)
	case configured.ModelID != "":
		profile, err = readCachedHuggingFaceModel(configured.ModelID, requestedContext)
	default:
		err = errors.New("managed engine has no configured model")
	}
	if err != nil {
		return shard.ModelProfile{}, err
	}
	profile.Name = configuredModelName(configured)
	if engine.modelName != "" {
		profile.Name = engine.modelName
	}
	if _, err := profile.EffectiveWeightsSizeBytes(); err != nil {
		return shard.ModelProfile{}, fmt.Errorf("model weights cannot be sized safely: %w", err)
	}
	return profile, nil
}

func configuredModelName(configured config.ManagedEngineConfig) string {
	if configured.ModelID != "" {
		return configured.ModelID
	}
	name := filepath.Base(configured.ModelPath)
	if strings.EqualFold(filepath.Ext(name), ".gguf") {
		return strings.TrimSuffix(name, filepath.Ext(name))
	}
	return name
}

func readConfiguredModelPath(path string, modelAllowlist []string, requestedContext int) (shard.ModelProfile, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("resolve configured model path: %w", err)
	}
	allowedRoot, err := approvedModelRoot(resolved, modelAllowlist)
	if err != nil {
		return shard.ModelProfile{}, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("stat configured model path: %w", err)
	}
	if info.IsDir() {
		return readHuggingFaceDirectory(resolved, allowedRoot, requestedContext)
	}
	if !strings.EqualFold(filepath.Ext(resolved), ".gguf") {
		return shard.ModelProfile{}, errors.New("configured model file is not a GGUF file")
	}
	file, err := os.Open(resolved)
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("open configured GGUF model: %w", err)
	}
	defer closeutil.Discard(file)
	profile, err := shard.ParseGGUFWithFileSize(file, uint64(info.Size()))
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("parse configured GGUF model: %w", err)
	}
	if requestedContext > 0 {
		profile.RequestedContext = requestedContext
	}
	return profile, nil
}

func approvedModelRoot(modelPath string, modelAllowlist []string) (string, error) {
	var best string
	for _, root := range modelAllowlist {
		resolved, err := filepath.EvalSymlinks(filepath.Clean(root))
		if err != nil {
			return "", fmt.Errorf("resolve approved model directory: %w", err)
		}
		if pathWithin(resolved, modelPath) && len(resolved) > len(best) {
			best = resolved
		}
	}
	if best == "" {
		return "", errors.New("configured model path is outside the approved model directories")
	}
	return best, nil
}

func readCachedHuggingFaceModel(modelID string, requestedContext int) (shard.ModelProfile, error) {
	if err := validateHuggingFaceModelID(modelID); err != nil {
		return shard.ModelProfile{}, err
	}
	cacheRoots, err := huggingFaceCacheRoots()
	if err != nil {
		return shard.ModelProfile{}, err
	}
	repository := "models--" + strings.ReplaceAll(modelID, "/", "--")
	for _, root := range cacheRoots {
		repositoryPath := filepath.Join(root, repository)
		snapshotsPath := filepath.Join(repositoryPath, "snapshots")
		entries, readErr := os.ReadDir(snapshotsPath)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return shard.ModelProfile{}, fmt.Errorf("list cached model revisions: %w", readErr)
		}
		type cachedRevision struct {
			entry    os.DirEntry
			modified os.FileInfo
		}
		revisions := make([]cachedRevision, 0, len(entries))
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			info, infoErr := entry.Info()
			if infoErr != nil {
				return shard.ModelProfile{}, fmt.Errorf("inspect cached model revision %q: %w", entry.Name(), infoErr)
			}
			revisions = append(revisions, cachedRevision{entry: entry, modified: info})
		}
		sort.SliceStable(revisions, func(i, j int) bool {
			return revisions[i].modified.ModTime().After(revisions[j].modified.ModTime())
		})
		var lastErr error
		for _, revision := range revisions {
			snapshot := filepath.Join(snapshotsPath, revision.entry.Name())
			profile, profileErr := readHuggingFaceDirectory(snapshot, root, requestedContext)
			if profileErr == nil {
				profile.Name = modelID
				return profile, nil
			}
			lastErr = profileErr
		}
		if lastErr != nil {
			return shard.ModelProfile{}, fmt.Errorf("read cached model %q: %w", modelID, lastErr)
		}
	}
	return shard.ModelProfile{}, fmt.Errorf("model %q is not cached on this node", modelID)
}

func validateHuggingFaceModelID(modelID string) error {
	if modelID == "" || strings.ContainsAny(modelID, "\\:\x00\r\n") {
		return errors.New("configured model ID is not a valid Hugging Face repository ID")
	}
	for _, part := range strings.Split(modelID, "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("configured model ID is not a valid Hugging Face repository ID")
		}
	}
	return nil
}

func huggingFaceCacheRoots() ([]string, error) {
	var roots []string
	if cache := strings.TrimSpace(os.Getenv("HF_HUB_CACHE")); cache != "" {
		roots = append(roots, cache)
	}
	if home := strings.TrimSpace(os.Getenv("HF_HOME")); home != "" {
		roots = append(roots, filepath.Join(home, "hub"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		roots = append(roots, filepath.Join(home, ".cache", "huggingface", "hub"))
	} else if len(roots) == 0 {
		return nil, fmt.Errorf("locate model cache home: %w", err)
	}
	unique := make([]string, 0, len(roots))
	seen := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		root = filepath.Clean(root)
		key := root
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, root)
	}
	return unique, nil
}

func readHuggingFaceDirectory(directory, allowedRoot string, requestedContext int) (shard.ModelProfile, error) {
	resolvedRoot, err := filepath.EvalSymlinks(allowedRoot)
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("resolve model metadata root: %w", err)
	}
	configPath := filepath.Join(directory, "config.json")
	resolvedConfig, err := filepath.EvalSymlinks(configPath)
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("resolve model config.json: %w", err)
	}
	if !pathWithin(resolvedRoot, resolvedConfig) {
		return shard.ModelProfile{}, errors.New("model config resolves outside its approved directory")
	}
	file, err := os.Open(resolvedConfig)
	if err != nil {
		return shard.ModelProfile{}, fmt.Errorf("open model config.json: %w", err)
	}
	profile, parseErr := shard.ParseHuggingFaceConfigWithContext(file, requestedContext)
	closeErr := file.Close()
	if parseErr != nil {
		return shard.ModelProfile{}, fmt.Errorf("parse model config.json: %w", parseErr)
	}
	if closeErr != nil {
		return shard.ModelProfile{}, fmt.Errorf("close model config.json: %w", closeErr)
	}
	weightBytes, err := huggingFaceWeightBytes(directory, resolvedRoot)
	if err != nil {
		return shard.ModelProfile{}, err
	}
	if weightBytes > 0 {
		profile.FileSizeBytes = weightBytes
	}
	if _, err := profile.EffectiveWeightsSizeBytes(); err != nil {
		return shard.ModelProfile{}, fmt.Errorf("model weights cannot be sized safely: %w", err)
	}
	return profile, nil
}

func huggingFaceWeightBytes(directory, allowedRoot string) (uint64, error) {
	var total uint64
	count := 0
	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("inspect model weight file: %w", walkErr)
		}
		if entry.IsDir() || !isHuggingFaceWeightFile(path) {
			return nil
		}
		count++
		if count > maxProfileModelFiles {
			return fmt.Errorf("model has more than %d weight files", maxProfileModelFiles)
		}
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("resolve model weight file: %w", err)
		}
		if !pathWithin(allowedRoot, resolved) {
			return errors.New("model weight file resolves outside its approved directory")
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return fmt.Errorf("stat model weight file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return errors.New("model weight path is not a regular file")
		}
		if ^uint64(0)-total < uint64(info.Size()) {
			return errors.New("model weight size overflows")
		}
		total += uint64(info.Size())
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

func isHuggingFaceWeightFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".safetensors", ".bin", ".pt", ".pth":
		return true
	default:
		return false
	}
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator)) &&
		!filepath.IsAbs(relative)
}
