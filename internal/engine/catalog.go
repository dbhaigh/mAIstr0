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
	{Name: "deepseek-r1:1.5b", Engine: "ollama", Description: "Compact distilled reasoning for lightweight step-by-step problem solving.", Tags: []string{"reasoning", "analysis", "small", "fast"}},
	{Name: "deepseek-r1:7b", Engine: "ollama", Description: "Distilled reasoning for everyday analysis, math, and coding tasks.", Tags: []string{"reasoning", "analysis", "math", "code"}},
	{Name: "deepseek-r1:8b", Engine: "ollama", Description: "Distilled reasoning for everyday analysis, math, and coding tasks.", Tags: []string{"reasoning", "analysis", "math", "code"}},
	{Name: "deepseek-r1:14b", Engine: "ollama", Description: "Larger distilled model for more involved reasoning, math, and code questions.", Tags: []string{"reasoning", "analysis", "math", "code"}},
	{Name: "deepseek-r1:32b", Engine: "ollama", Description: "High-capacity distilled reasoning model for complex analysis and coding.", Tags: []string{"reasoning", "analysis", "math", "code"}},
	{Name: "deepseek-r1:70b", Engine: "ollama", Description: "Large distilled reasoning model for complex multi-step analysis and coding.", Tags: []string{"reasoning", "analysis", "math", "code", "large"}},
	{Name: "gemma3:1b", Engine: "ollama", Description: "Compact text model for quick chat, rewriting, and lightweight classification.", Tags: []string{"general", "chat", "classification", "small", "fast"}},
	{Name: "gemma3:4b", Engine: "ollama", Description: "Compact chat model with image understanding for everyday questions and summaries.", Tags: []string{"general", "chat", "vision", "small"}},
	{Name: "gemma3:12b", Engine: "ollama", Description: "Multimodal assistant for chat, image understanding, and document summarization.", Tags: []string{"general", "chat", "vision", "summarization"}},
	{Name: "gemma3:27b", Engine: "ollama", Description: "Higher-capacity multimodal assistant for detailed chat, image understanding, and summaries.", Tags: []string{"general", "chat", "vision", "summarization", "large"}},
	{Name: "llama3.1:8b", Engine: "ollama", Description: "Versatile instruction model for chat, summarization, and common coding tasks.", Tags: []string{"general", "chat", "summarization", "code", "multilingual"}},
	{Name: "llama3.1:70b", Engine: "ollama", Description: "Large instruction model for detailed analysis, writing, and coding.", Tags: []string{"general", "analysis", "writing", "code", "multilingual", "large"}},
	{Name: "llama3.2:1b", Engine: "ollama", Description: "Small text model for quick responses, extraction, and lightweight assistant tasks.", Tags: []string{"general", "chat", "extraction", "small", "fast"}},
	{Name: "llama3.2:3b", Engine: "ollama", Description: "Efficient text assistant for chat, summaries, and everyday question answering.", Tags: []string{"general", "chat", "summarization", "small"}},
	{Name: "llama3.2-vision:11b", Engine: "ollama", Description: "Vision-language assistant for image questions, visual analysis, and text-based chat.", Tags: []string{"vision", "image", "general", "analysis"}},
	{Name: "mistral:7b", Engine: "ollama", Description: "General instruction model for chat, concise writing, and code assistance.", Tags: []string{"general", "chat", "writing", "code"}},
	{Name: "mistral-nemo:12b", Engine: "ollama", Description: "Multilingual assistant for longer conversations, writing, and code tasks.", Tags: []string{"general", "chat", "multilingual", "writing", "code"}},
	{Name: "phi4-mini", Engine: "ollama", Description: "Small reasoning and coding model suited to fast local assistant tasks.", Tags: []string{"reasoning", "code", "math", "small", "fast"}},
	{Name: "phi4:14b", Engine: "ollama", Description: "Reasoning-focused model for math, structured answers, and code generation.", Tags: []string{"reasoning", "math", "code", "structured-output"}},
	{Name: "qwen2.5:0.5b", Engine: "ollama", Description: "Very compact multilingual model for simple chat, extraction, and classification.", Tags: []string{"general", "chat", "multilingual", "classification", "small", "fast"}},
	{Name: "qwen2.5:1.5b", Engine: "ollama", Description: "Small multilingual assistant for quick chat, extraction, and simple coding.", Tags: []string{"general", "chat", "multilingual", "code", "small", "fast"}},
	{Name: "qwen2.5:3b", Engine: "ollama", Description: "Compact multilingual assistant for everyday chat, summaries, and basic code.", Tags: []string{"general", "chat", "multilingual", "summarization", "code", "small"}},
	{Name: "qwen2.5:7b", Engine: "ollama", Description: "Balanced multilingual assistant for chat, coding, and structured text tasks.", Tags: []string{"general", "chat", "multilingual", "code", "structured-output"}},
	{Name: "qwen2.5:14b", Engine: "ollama", Description: "Higher-capacity multilingual model for analysis, code, and structured responses.", Tags: []string{"general", "analysis", "multilingual", "code", "structured-output"}},
	{Name: "qwen2.5:32b", Engine: "ollama", Description: "Large multilingual model for demanding analysis, code, and instruction-following.", Tags: []string{"general", "analysis", "multilingual", "code", "large"}},
	{Name: "qwen2.5-coder:0.5b", Engine: "ollama", Description: "Tiny code model for lightweight completion and programming experiments.", Tags: []string{"code", "completion", "small", "fast"}},
	{Name: "qwen2.5-coder:1.5b", Engine: "ollama", Description: "Small code model for quick code completion, explanation, and simple edits.", Tags: []string{"code", "completion", "small", "fast"}},
	{Name: "qwen2.5-coder:3b", Engine: "ollama", Description: "Compact code assistant for completion, explanations, and routine edits.", Tags: []string{"code", "completion", "small"}},
	{Name: "qwen2.5-coder:7b", Engine: "ollama", Description: "Balanced code assistant for generation, debugging, and programming questions.", Tags: []string{"code", "generation", "debugging"}},
	{Name: "qwen2.5-coder:14b", Engine: "ollama", Description: "Code model for multi-file changes, debugging, and detailed programming help.", Tags: []string{"code", "generation", "debugging", "analysis"}},
	{Name: "qwen2.5-coder:32b", Engine: "ollama", Description: "Large code model for complex generation, debugging, and repository-level tasks.", Tags: []string{"code", "generation", "debugging", "analysis", "large"}},
	{Name: "qwen3:0.6b", Engine: "ollama", Description: "Very compact model for fast chat, simple reasoning, and low-resource nodes.", Tags: []string{"general", "reasoning", "small", "fast"}},
	{Name: "qwen3:1.7b", Engine: "ollama", Description: "Small multilingual model for quick chat, simple reasoning, and lightweight code.", Tags: []string{"general", "reasoning", "multilingual", "code", "small", "fast"}},
	{Name: "qwen3:4b", Engine: "ollama", Description: "Compact reasoning-capable assistant for chat, math, multilingual tasks, and code.", Tags: []string{"general", "reasoning", "math", "multilingual", "code", "small"}},
	{Name: "qwen3:8b", Engine: "ollama", Description: "Balanced model for reasoning, multilingual chat, coding, and general assistance.", Tags: []string{"general", "reasoning", "math", "multilingual", "code"}},
	{Name: "qwen3:14b", Engine: "ollama", Description: "Higher-capacity model for detailed reasoning, coding, and multilingual tasks.", Tags: []string{"general", "reasoning", "math", "multilingual", "code"}},
	{Name: "qwen3:30b-a3b", Engine: "ollama", Description: "Mixture-of-experts model for capable reasoning, code, and general assistance.", Tags: []string{"general", "reasoning", "math", "multilingual", "code", "mixture-of-experts"}},
	{Name: "qwen3:32b", Engine: "ollama", Description: "Large dense model for demanding reasoning, coding, and multilingual work.", Tags: []string{"general", "reasoning", "math", "multilingual", "code", "large"}},
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
