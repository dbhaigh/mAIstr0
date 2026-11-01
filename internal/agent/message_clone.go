package agent

// CloneMessage copies the message and its nested tool-call and response structs.
func CloneMessage(message Message) Message {
	if message.ToolCalls != nil {
		toolCalls := make([]ToolCall, len(message.ToolCalls))
		copy(toolCalls, message.ToolCalls)
		message.ToolCalls = toolCalls
	}
	if message.ToolResponse != nil {
		response := *message.ToolResponse
		message.ToolResponse = &response
	}
	return message
}

// CloneStreamEvent copies the event and its nested message and tool structs.
func CloneStreamEvent(event StreamEvent) StreamEvent {
	if event.Message != nil {
		message := CloneMessage(*event.Message)
		event.Message = &message
	}
	if event.ToolCall != nil {
		call := *event.ToolCall
		event.ToolCall = &call
	}
	if event.ToolResp != nil {
		response := *event.ToolResp
		event.ToolResp = &response
	}
	return event
}
