package orchestrator

import (
	"bytes"
	"encoding/json"
)

type compatStreamEvent struct {
	Output string `json:"output"`
	Error  string `json:"error"`
}

type compatStreamParser struct {
	buffer []byte
}

func decodeCompatStreamEvent(payload []byte) (compatStreamEvent, error) {
	var event compatStreamEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return compatStreamEvent{}, err
	}
	return event, nil
}

func (p *compatStreamParser) write(data []byte, consume func([]byte) error) error {
	p.buffer = append(p.buffer, data...)
	consumed := 0
	for {
		newline := bytes.IndexByte(p.buffer[consumed:], '\n')
		if newline < 0 {
			break
		}
		end := consumed + newline
		line := bytes.TrimSpace(p.buffer[consumed:end])
		consumed = end + 1
		if bytes.HasPrefix(line, []byte("data:")) {
			if err := consume(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))); err != nil {
				p.discard(consumed)
				return err
			}
		}
	}
	p.discard(consumed)
	return nil
}

func (p *compatStreamParser) flushPending(consume func([]byte) error) error {
	rest := bytes.TrimSpace(p.buffer)
	if !bytes.HasPrefix(rest, []byte("data:")) {
		return nil
	}
	return consume(bytes.TrimSpace(bytes.TrimPrefix(rest, []byte("data:"))))
}

func (p *compatStreamParser) discard(consumed int) {
	if consumed == 0 {
		return
	}
	remaining := copy(p.buffer, p.buffer[consumed:])
	p.buffer = p.buffer[:remaining]
}
