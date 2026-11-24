package execution

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// ServiceConfig describes an explicitly owned, long-running local process.
// The process receives only the configured environment and runs in the
// operator-approved working directory.
type ServiceConfig struct {
	Executable       string
	Args             []string
	WorkingDirectory string
	Environment      map[string]string
}

// ServiceProcess owns a process tree until Close is called.
type ServiceProcess struct {
	tree processTree
	done chan struct{}

	mu       sync.Mutex
	closing  bool
	waitErr  error
	closeErr error

	closeOnce sync.Once
}

// StartServiceProcess starts an explicitly configured long-running process.
// Stdout and stderr are discarded so child output cannot leak prompts,
// credentials, or tool output into application logs.
func StartServiceProcess(config ServiceConfig) (*ServiceProcess, error) {
	if !processTreeSupported() {
		return nil, ErrProcessTreeUnsupported
	}
	workspace, err := canonicalWorkspace(config.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	if !filepath.IsAbs(config.Executable) || strings.ContainsRune(config.Executable, '\x00') {
		return nil, ErrInvalidConfig
	}
	executable, err := filepath.EvalSymlinks(config.Executable)
	if err != nil {
		return nil, ErrProcessStart
	}
	info, err := os.Stat(executable)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrProcessStart
	}
	for _, arg := range config.Args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, ErrInvalidConfig
		}
	}
	for name, value := range config.Environment {
		if !validEnvironmentName(name) || strings.ContainsRune(value, '\x00') {
			return nil, ErrInvalidConfig
		}
		if runtime.GOOS == "windows" && strings.EqualFold(name, "SystemRoot") {
			return nil, ErrInvalidConfig
		}
	}

	command := exec.Command(executable, config.Args...)
	command.Dir = workspace
	command.Env = processEnvironment(config.Environment)
	command.Stdin = strings.NewReader("")
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	tree, err := startProcessTree(command)
	if err != nil {
		return nil, ErrProcessStart
	}
	process := &ServiceProcess{tree: tree, done: make(chan struct{})}
	go func() {
		waitErr := command.Wait()
		closeErr := tree.Close()
		process.mu.Lock()
		if waitErr != nil && !process.closing && !errors.Is(waitErr, os.ErrProcessDone) {
			process.waitErr = ErrProcessFailed
		}
		if closeErr != nil {
			process.closeErr = ErrProcessTerminationFailed
		}
		process.mu.Unlock()
		close(process.done)
	}()
	return process, nil
}

// Done is closed when the owned process has exited and its process tree has
// been reaped.
func (p *ServiceProcess) Done() <-chan struct{} { return p.done }

// Status reports whether the process has exited, without exposing child output.
func (p *ServiceProcess) Status() (exited bool, err error) {
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.waitErr != nil {
			return true, p.waitErr
		}
		if p.closeErr != nil {
			return true, p.closeErr
		}
		return true, nil
	default:
		return false, nil
	}
}

// Close terminates and joins the owned process tree.
func (p *ServiceProcess) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()
		select {
		case <-p.done:
		default:
			if err := p.tree.Kill(); err != nil {
				p.mu.Lock()
				p.closeErr = ErrProcessTerminationFailed
				p.mu.Unlock()
			}
		}
		<-p.done
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	return errors.Join(p.waitErr, p.closeErr)
}
