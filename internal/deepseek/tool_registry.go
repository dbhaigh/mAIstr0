package deepseek

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]registeredTool
}

type registeredTool struct {
	tool         Tool
	argumentType reflect.Type
}

func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]registeredTool)}
}

func (r *ToolRegistry) Register(tool Tool) {
	r.register(tool, nil)
}

func registerTypedTool[Args any](registry *ToolRegistry, tool Tool) {
	argumentType := reflect.TypeOf((*Args)(nil)).Elem()
	registry.register(tool, argumentType)
}

func (r *ToolRegistry) register(tool Tool, argumentType reflect.Type) {
	definition := tool.Def()
	r.mu.Lock()
	r.tools[definition.Name] = registeredTool{tool: tool, argumentType: argumentType}
	r.mu.Unlock()
}

func (r *ToolRegistry) Get(name string) (Tool, bool) {
	r.mu.RLock()
	entry, ok := r.tools[name]
	r.mu.RUnlock()
	return entry.tool, ok
}

func (r *ToolRegistry) List() []ToolDef {
	r.mu.RLock()
	tools := make([]Tool, 0, len(r.tools))
	for _, entry := range r.tools {
		tools = append(tools, entry.tool)
	}
	r.mu.RUnlock()

	definitions := make([]ToolDef, 0, len(tools))
	for _, tool := range tools {
		definitions = append(definitions, tool.Def())
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Name < definitions[j].Name
	})
	return definitions
}

func toolDefinition[Args any](name, description string) ToolDef {
	argsType := reflect.TypeOf((*Args)(nil)).Elem()
	return ToolDef{
		Name: name, Description: description,
		Parameters: schemaForType(argsType),
	}
}

type toolRegistration struct {
	tool         Tool
	argumentType reflect.Type
}

func typedToolRegistration[Args any](tool Tool) toolRegistration {
	return toolRegistration{
		tool: tool, argumentType: reflect.TypeOf((*Args)(nil)).Elem(),
	}
}

func schemaForType(value reflect.Type) map[string]any {
	for value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}

	properties := make(map[string]any, value.NumField())
	required := make([]string, 0, value.NumField())
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if !field.IsExported() {
			continue
		}
		name, options, ok := jsonField(field)
		if !ok {
			continue
		}
		property := schemaForField(field.Type)
		metadata := parseSchemaMetadata(field.Tag.Get("jsonschema"))
		for key, annotation := range metadata {
			property[key] = annotation
		}
		properties[name] = property
		if !options["omitempty"] {
			required = append(required, name)
		}
	}

	schema := map[string]any{"type": "object", "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func schemaForField(value reflect.Type) map[string]any {
	for value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{"type": "array", "items": schemaForField(value.Elem())}
	case reflect.Struct:
		return schemaForType(value)
	case reflect.Map:
		return map[string]any{"type": "object"}
	default:
		return map[string]any{}
	}
}

func jsonField(field reflect.StructField) (string, map[string]bool, bool) {
	tag := field.Tag.Get("json")
	parts := strings.Split(tag, ",")
	name := parts[0]
	if name == "-" {
		return "", nil, false
	}
	if name == "" {
		name = strings.ToLower(field.Name)
	}
	options := make(map[string]bool, len(parts)-1)
	for _, option := range parts[1:] {
		options[option] = true
	}
	return name, options, true
}

func parseSchemaMetadata(tag string) map[string]any {
	metadata := make(map[string]any)
	for _, part := range strings.Split(tag, ";") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch key {
		case "description":
			metadata[key] = value
		case "enum":
			metadata[key] = strings.Split(value, "|")
		case "minimum", "maximum":
			if number, err := strconv.ParseFloat(value, 64); err == nil {
				metadata[key] = number
			}
		}
	}
	return metadata
}

type workflowSubtaskArgs struct {
	ID             string   `json:"id,omitempty"`
	Description    string   `json:"description"`
	Role           string   `json:"role,omitempty" jsonschema:"enum=researcher|analyst|coder|writer|reviewer|summarizer|translator|general"`
	TaskType       string   `json:"task_type,omitempty" jsonschema:"enum=code|summarize|translate|math|vision|creative|general"`
	DependsOn      []string `json:"depends_on,omitempty"`
	ExpectedOutput string   `json:"expected_output,omitempty"`
	OutputFormat   string   `json:"output_format,omitempty" jsonschema:"enum=text|json"`
	RequiredFields []string `json:"required_fields,omitempty"`
	MaxRetries     *int     `json:"max_retries,omitempty" jsonschema:"minimum=0;maximum=2"`
}

type workflowArgs struct {
	Description   string                `json:"description" jsonschema:"description=The overall task to complete."`
	Subtasks      []workflowSubtaskArgs `json:"subtasks,omitempty" jsonschema:"description=Optional tasks with id, description, role, task_type, depends_on, expected_output, output_format, required_fields, and max_retries."`
	RequireReview bool                  `json:"require_review,omitempty" jsonschema:"description=Add a reviewer task that must approve the completed work before synthesis."`
}

type clusterStatusArgs struct{}

type nodeConverseArgs struct {
	NodeID       string `json:"node_id" jsonschema:"description=The target compute node ID (e.g. 'node-1', 'orchestrator-...')."`
	Message      string `json:"message" jsonschema:"description=The message, instruction, or follow-up question to send to the node's model."`
	DialogueID   string `json:"dialogue_id,omitempty" jsonschema:"description=Optional dialogue/conversation ID to continue an ongoing conversation with this node. If omitted, a new dialogue is started."`
	Model        string `json:"model,omitempty" jsonschema:"description=Optional specific model name to converse with on that node."`
	SystemPrompt string `json:"system_prompt,omitempty" jsonschema:"description=Optional system prompt setting the persona/role of the worker node model."`
}

type nodeCollaborateArgs struct {
	NodeID            string  `json:"node_id" jsonschema:"description=The target compute node ID to collaborate with."`
	Task              string  `json:"task" jsonschema:"description=The complex task or problem for the node to solve."`
	Model             string  `json:"model,omitempty" jsonschema:"description=Optional model name on that node."`
	CollaborationGoal string  `json:"collaboration_goal,omitempty" jsonschema:"description=Optional specific quality criteria or review guidelines."`
	Rounds            float64 `json:"rounds,omitempty" jsonschema:"description=Number of interactive feedback iterations (1 to 4, default 2)."`
}

type clusterConverseArgs struct {
	Message         string `json:"message" jsonschema:"description=The message or task instruction for the cluster node."`
	DialogueID      string `json:"dialogue_id,omitempty" jsonschema:"description=Optional dialogue ID to continue a conversation across turns."`
	TaskType        string `json:"task_type,omitempty" jsonschema:"description=Optional category tag (e.g. 'code', 'math', 'reasoning', 'chat', 'general')."`
	ModelPreference string `json:"model_preference,omitempty" jsonschema:"description=Optional model preference."`
}

type listNodeDialoguesArgs struct {
	NodeID string `json:"node_id,omitempty" jsonschema:"description=Optional node ID to filter dialogues for a specific node."`
}

type getNodeDialogueArgs struct {
	NodeID     string `json:"node_id" jsonschema:"description=The node ID where the dialogue is hosted."`
	DialogueID string `json:"dialogue_id" jsonschema:"description=The unique dialogue ID."`
}

type listNodeModelsArgs struct {
	NodeID string `json:"node_id,omitempty" jsonschema:"description=Optional node ID to filter models by a specific node. Omit to list models across all nodes."`
}

type nodeLLMQueryArgs struct {
	NodeID string `json:"node_id" jsonschema:"description=The unique ID of the target compute node (e.g. 'node-1', 'orchestrator-...')."`
	Model  string `json:"model,omitempty" jsonschema:"description=Optional model name on that node. If omitted, uses the node's default model or first installed model."`
	Prompt string `json:"prompt" jsonschema:"description=The prompt or task instruction to send to the node's LLM."`
	System string `json:"system,omitempty" jsonschema:"description=Optional system prompt instruction for the LLM on that node."`
}

type testNodeModelArgs struct {
	NodeID string `json:"node_id" jsonschema:"description=The node ID where the model is hosted."`
	Model  string `json:"model" jsonschema:"description=The name of the model to test on that node."`
	Prompt string `json:"prompt,omitempty" jsonschema:"description=Optional test prompt (defaults to 'Respond with OK in 3 words.')."`
}

type clusterLLMQueryArgs struct {
	Prompt          string `json:"prompt" jsonschema:"description=The prompt or instruction to execute on the cluster node."`
	TaskType        string `json:"task_type,omitempty" jsonschema:"description=Optional category to match specialized models (e.g. 'code', 'math', 'reasoning', 'summarize', 'vision', 'fast', 'general')."`
	ModelPreference string `json:"model_preference,omitempty" jsonschema:"description=Optional specific model name to target if present on a cluster node."`
}

type parallelSubtaskArgs struct {
	Description     string `json:"description" jsonschema:"description=Instruction or prompt for this subtask."`
	TaskType        string `json:"task_type,omitempty" jsonschema:"description=Optional category (e.g. 'code', 'math', 'reasoning', 'summarize', 'general')."`
	ModelPreference string `json:"model_preference,omitempty" jsonschema:"description=Optional specific model name preference."`
}

type clusterParallelDispatchArgs struct {
	Subtasks []parallelSubtaskArgs `json:"subtasks" jsonschema:"description=List of subtasks to fan out in parallel across cluster nodes."`
}

type evalExpressionArgs struct {
	Expression string `json:"expression" jsonschema:"description=The mathematical or statistical expression to evaluate (e.g. '144 * 28 + 195', 'sqrt(256)', 'mean(12, 18, 25, 40)', 'sum(100, 250, 450)')."`
}

type fetchWebArgs struct {
	URL string `json:"url" jsonschema:"description=The URL to fetch (HTTP or HTTPS)."`
}

type sessionMemoryArgs struct {
	Action string `json:"action" jsonschema:"enum=get|set|list|delete;description=The operation to perform on session memory."`
	Key    string `json:"key,omitempty" jsonschema:"description=Key identifier for the stored item."`
	Value  string `json:"value,omitempty" jsonschema:"description=Value to store when action is 'set'."`
}

type createSubAgentArgs struct {
	Name                  string   `json:"name" jsonschema:"description=Identifier or title for the specialized sub-agent (e.g. 'sql-expert', 'patent-analyst')."`
	Role                  string   `json:"role,omitempty" jsonschema:"description=Optional role name for the worker profile (defaults to name)."`
	Instructions          string   `json:"instructions" jsonschema:"description=System prompt / persona instructions guiding how the worker behaves."`
	RequiredTools         []string `json:"required_tools,omitempty" jsonschema:"description=Optional list of tool names the sub-agent needs access to."`
	RequiredCapabilities  []string `json:"required_capabilities,omitempty" jsonschema:"description=Capabilities the worker's node/model must support (e.g. 'coder', 'researcher', 'vision', 'math', 'long-context', 'fast', 'high-quality')."`
	PreferredCapabilities []string `json:"preferred_capabilities,omitempty" jsonschema:"description=Optional bonus capabilities preferred for this sub-agent."`
	Context               string   `json:"context,omitempty" jsonschema:"description=Domain knowledge or background context for this sub-agent."`
	Task                  string   `json:"task,omitempty" jsonschema:"description=Optional prompt/task to immediately execute with the created sub-agent."`
}

type recallMemoryArgs struct {
	Query       string  `json:"query" jsonschema:"description=Keywords describing what to remember (e.g. 'go worker pool benchmark')."`
	TaskType    string  `json:"task_type,omitempty" jsonschema:"description=Optional task category filter (code, math, reasoning, summarize, general)."`
	NodeID      string  `json:"node_id,omitempty" jsonschema:"description=Optional filter to experiences on one node."`
	SuccessOnly bool    `json:"success_only,omitempty" jsonschema:"description=When true, only recall runs that succeeded."`
	Limit       float64 `json:"limit,omitempty" jsonschema:"description=Maximum memories to return (default 8)."`
}

type rememberFactArgs struct {
	Key   string `json:"key" jsonschema:"description=Short identifier for the fact (e.g. 'preferred-code-model')."`
	Value string `json:"value" jsonschema:"description=The knowledge to remember."`
	Scope string `json:"scope,omitempty" jsonschema:"description=Optional scope: 'cluster' (default), 'node:<id>', or 'session:<id>'."`
}

type memoryInsightsArgs struct {
	TaskType string `json:"task_type,omitempty" jsonschema:"description=Optional task category to focus the recommendations on."`
}

type nodeToNodeConversationArgs struct {
	NodeA   string  `json:"node_a" jsonschema:"description=Node ID whose LLM opens the conversation."`
	NodeB   string  `json:"node_b" jsonschema:"description=Node ID whose LLM responds."`
	Topic   string  `json:"topic" jsonschema:"description=What the two node LLMs should discuss or decide."`
	Opening string  `json:"opening,omitempty" jsonschema:"description=Optional opening instruction for the first node's LLM."`
	Rounds  float64 `json:"rounds,omitempty" jsonschema:"description=Number of back-and-forth rounds (1 to 5, default 2)."`
}
