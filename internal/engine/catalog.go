package engine

import "strings"

// CatalogModel describes a curated, locally bundled model-catalog entry.
type CatalogModel struct {
	Name        string   `json:"name"`
	Engine      string   `json:"engine"`
	Description string   `json:"description"`
	Tags        []string `json:"tags,omitempty"`
}

var modelCatalog = []CatalogModel{
	{Name: "deepseek-r1:1.5b", Engine: "ollama", Description: "Small reasoning model", Tags: []string{"reasoning", "general"}},
	{Name: "deepseek-r1:7b", Engine: "ollama", Description: "Reasoning model", Tags: []string{"reasoning", "general"}},
	{Name: "deepseek-r1:8b", Engine: "ollama", Description: "Reasoning model", Tags: []string{"reasoning", "general"}},
	{Name: "gemma3:1b", Engine: "ollama", Description: "Small general-purpose model", Tags: []string{"general", "fast"}},
	{Name: "gemma3:4b", Engine: "ollama", Description: "General-purpose model", Tags: []string{"general", "vision"}},
	{Name: "llama3.2:1b", Engine: "ollama", Description: "Small general-purpose model", Tags: []string{"general", "fast"}},
	{Name: "llama3.2:3b", Engine: "ollama", Description: "General-purpose model", Tags: []string{"general"}},
	{Name: "mistral:7b", Engine: "ollama", Description: "General-purpose model", Tags: []string{"general", "code"}},
	{Name: "phi4-mini", Engine: "ollama", Description: "Small reasoning and coding model", Tags: []string{"reasoning", "code"}},
	{Name: "qwen2.5:0.5b", Engine: "ollama", Description: "Compact general-purpose model", Tags: []string{"general", "fast"}},
	{Name: "qwen2.5:1.5b", Engine: "ollama", Description: "Small general-purpose model", Tags: []string{"general", "fast"}},
	{Name: "qwen2.5:3b", Engine: "ollama", Description: "General-purpose model", Tags: []string{"general"}},
	{Name: "qwen2.5:7b", Engine: "ollama", Description: "General-purpose and coding model", Tags: []string{"general", "code"}},
}

// ModelCatalog returns a copy of curated catalog entries, optionally filtered
// by engine and a case-insensitive name or tag query.
func ModelCatalog(engineName, query string) []CatalogModel {
	engineName = strings.ToLower(strings.TrimSpace(engineName))
	query = strings.ToLower(strings.TrimSpace(query))
	var models []CatalogModel
	for _, model := range modelCatalog {
		if engineName != "" && !strings.EqualFold(model.Engine, engineName) {
			continue
		}
		if query != "" {
			matched := strings.Contains(strings.ToLower(model.Name), query) ||
				strings.Contains(strings.ToLower(model.Description), query)
			for _, tag := range model.Tags {
				matched = matched || strings.Contains(strings.ToLower(tag), query)
			}
			if !matched {
				continue
			}
		}
		model.Tags = append([]string(nil), model.Tags...)
		models = append(models, model)
	}
	return models
}
