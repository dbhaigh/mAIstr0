package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/maistr0/maistr0/internal/shard"
)

type LocalHeadModel struct {
	ID      string             `json:"id"`
	Name    string             `json:"name"`
	Source  string             `json:"source"`
	Profile shard.ModelProfile `json:"profile"`
	path    string
	root    string
}

type LocalHeadModels struct {
	Models   []LocalHeadModel `json:"models"`
	Warnings []string         `json:"warnings"`
}

var ollamaDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var splitGGUFName = regexp.MustCompile(`(?i)-[0-9]{5}-of-[0-9]{5}\.gguf$`)

func localModelRoots() (map[string][]string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	ollama := os.Getenv("OLLAMA_MODELS")
	if ollama == "" {
		ollama = filepath.Join(home, ".ollama", "models")
	}
	lm := []string{filepath.Join(home, ".lmstudio", "models"), filepath.Join(home, ".cache", "lm-studio", "models")}
	if configured := os.Getenv("LMSTUDIO_MODELS"); configured != "" {
		lm = append(lm, configured)
	}
	return map[string][]string{"ollama": {ollama}, "lmstudio": lm}, nil
}

func (m *Manager) LocalHeadModels(ctx context.Context, requestedContext int) (LocalHeadModels, error) {
	roots, err := localModelRoots()
	if err != nil {
		return LocalHeadModels{}, err
	}
	m.mu.Lock()
	if m.workerRoot != "" {
		roots["huggingface"] = []string{filepath.Join(m.workerRoot, "models")}
	}
	m.mu.Unlock()
	return discoverLocalHeadModels(ctx, roots, requestedContext)
}

func discoverLocalHeadModels(ctx context.Context, roots map[string][]string, requestedContext int) (LocalHeadModels, error) {
	result := LocalHeadModels{Models: []LocalHeadModel{}, Warnings: []string{}}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if requestedContext < 1 || requestedContext > 1<<20 {
		return result, errors.New("requested context must be from 1 to 1048576")
	}
	seen := map[string]bool{}
	for _, source := range []string{"ollama", "lmstudio", "huggingface"} {
		directories := roots[source]
		for _, directory := range directories {
			root, err := filepath.EvalSymlinks(directory)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				result.Warnings = append(result.Warnings, source+": model directory is unreadable")
				continue
			}
			root, err = filepath.Abs(root)
			if err != nil {
				return result, err
			}
			scan := root
			if source == "ollama" {
				scan = filepath.Join(root, "manifests")
			}
			count := 0
			err = filepath.WalkDir(scan, func(path string, entry os.DirEntry, walkErr error) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if walkErr != nil {
					if !errors.Is(walkErr, os.ErrNotExist) {
						result.Warnings = append(result.Warnings, source+": part of the model inventory is unreadable")
					}
					if entry != nil && entry.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				count++
				if count > maxProfileModelFiles {
					return errors.New("local model inventory exceeds scan limit")
				}
				if entry.IsDir() {
					return nil
				}
				name := strings.TrimSuffix(entry.Name(), filepath.Ext(entry.Name()))
				modelPath := path
				if source == "ollama" {
					if _, err := approvedModelRoot(path, []string{root}); err != nil {
						result.Warnings = append(result.Warnings, "ollama: manifest outside its inventory root was rejected")
						return nil
					}
					manifest, err := os.Open(path)
					if err != nil {
						result.Warnings = append(result.Warnings, "ollama: could not read a model manifest")
						return nil
					}
					var record struct {
						Layers []struct {
							MediaType string `json:"mediaType"`
							Digest    string `json:"digest"`
						} `json:"layers"`
					}
					err = json.NewDecoder(io.LimitReader(manifest, 1<<20)).Decode(&record)
					closeErr := manifest.Close()
					if errors.Join(err, closeErr) != nil {
						result.Warnings = append(result.Warnings, "ollama: invalid model manifest")
						return nil
					}
					modelPath = ""
					for _, layer := range record.Layers {
						if layer.MediaType == "application/vnd.ollama.image.model" && ollamaDigest.MatchString(layer.Digest) {
							modelPath = filepath.Join(root, "blobs", strings.ReplaceAll(layer.Digest, ":", "-"))
							break
						}
					}
					if modelPath == "" {
						return nil
					}
					relative, err := filepath.Rel(scan, path)
					if err != nil {
						return err
					}
					modelName := strings.TrimPrefix(filepath.ToSlash(filepath.Dir(relative)), "registry.ollama.ai/")
					name = strings.TrimPrefix(modelName, "library/") + ":" + filepath.Base(relative)
				} else if !strings.EqualFold(filepath.Ext(path), ".gguf") {
					return nil
				}
				resolved, err := filepath.EvalSymlinks(modelPath)
				if err != nil {
					result.Warnings = append(result.Warnings, source+": model weight file is unavailable")
					return nil
				}
				if _, err := approvedModelRoot(resolved, []string{root}); err != nil {
					result.Warnings = append(result.Warnings, source+": model outside its inventory root was rejected")
					return nil
				}
				if seen[resolved] {
					return nil
				}
				profile, err := readLocalGGUF(resolved, requestedContext)
				if err != nil {
					result.Warnings = append(result.Warnings, source+": "+name+" is not a supported readable GGUF model")
					return nil
				}
				id := sha256.Sum256([]byte(resolved))
				profile.Name = name
				result.Models = append(result.Models, LocalHeadModel{
					ID: hex.EncodeToString(id[:]), Name: name, Source: source, Profile: profile, path: resolved, root: root,
				})
				seen[resolved] = true
				return nil
			})
			if err != nil {
				return result, fmt.Errorf("scan %s models: %w", source, err)
			}
		}
	}
	sort.Slice(result.Models, func(i, j int) bool { return result.Models[i].Name < result.Models[j].Name })
	sort.Strings(result.Warnings)
	return result, nil
}

func readLocalGGUF(path string, requestedContext int) (shard.ModelProfile, error) {
	if splitGGUFName.MatchString(filepath.Base(path)) {
		return shard.ModelProfile{}, errors.New("split GGUF files are not supported for automatic head setup")
	}
	file, err := os.Open(path)
	if err != nil {
		return shard.ModelProfile{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return shard.ModelProfile{}, errors.New("model is not a regular file")
	}
	profile, err := shard.ParseGGUFWithFileSize(file, uint64(info.Size()))
	if err != nil {
		return profile, err
	}
	profile.RequestedContext = requestedContext
	return profile, nil
}
