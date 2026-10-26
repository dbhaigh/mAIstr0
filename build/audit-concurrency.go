//go:build ignore

// Run with go run to regenerate the syntactic concurrency audit index.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type site struct {
	File       string `json:"file"`
	Line       int    `json:"line"`
	Function   string `json:"function"`
	Kind       string `json:"kind"`
	Expression string `json:"expression"`
}

func main() {
	root := flag.String("root", ".", "repository root")
	output := flag.String("output", "", "output JSON file")
	flag.Parse()
	if *output == "" {
		fmt.Fprintln(os.Stderr, "-output is required")
		os.Exit(2)
	}
	if err := census(*root, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func census(root, output string) error {
	files := token.NewFileSet()
	var sites []site
	fileCount := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "testdata", ".codex":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "build/audit-concurrency.go" {
			return nil
		}
		fileCount++
		tree, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", relative, err)
		}
		var formatErr error
		text := func(node ast.Node) string {
			var buffer bytes.Buffer
			if err := format.Node(&buffer, files, node); err != nil {
				formatErr = err
			}
			return buffer.String()
		}
		add := func(node ast.Node, function, kind, expression string) {
			sites = append(sites, site{relative, files.Position(node.Pos()).Line, function, kind, expression})
		}
		var name func(ast.Expr) string
		name = func(expression ast.Expr) string {
			switch value := expression.(type) {
			case *ast.FuncLit:
				return "func literal"
			case *ast.IndexExpr:
				return name(value.X)
			case *ast.IndexListExpr:
				return name(value.X)
			default:
				return text(expression)
			}
		}
		inspect := func(node ast.Node, function string) {
			ast.Inspect(node, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.GoStmt:
					add(value, function, "goroutine", name(value.Call.Fun))
				case *ast.DeferStmt:
					add(value, function, "defer", name(value.Call.Fun))
				case *ast.SendStmt:
					add(value, function, "channel_send", text(value.Chan))
				case *ast.UnaryExpr:
					if value.Op == token.ARROW {
						add(value, function, "channel_receive", text(value.X))
					}
				case *ast.SelectorExpr:
					if base, ok := value.X.(*ast.Ident); ok && (base.Name == "sync" || base.Name == "atomic") {
						switch value.Sel.Name {
						case "Mutex", "RWMutex", "Cond", "WaitGroup", "Once", "Pool", "Map",
							"Bool", "Int32", "Int64", "Uint32", "Uint64", "Pointer", "Value":
							add(value, function, "synchronization_type", text(value))
						}
					}
				case *ast.CallExpr:
					call := name(value.Fun)
					if call == "make" && len(value.Args) > 0 {
						if _, ok := value.Args[0].(*ast.ChanType); ok {
							capacity := "0"
							if len(value.Args) > 1 {
								capacity = text(value.Args[1])
							}
							add(value, function, "channel_make", text(value.Args[0])+" capacity="+capacity)
						}
					}
					kind := ""
					switch {
					case strings.HasPrefix(call, "context."):
						kind = "context"
					case strings.HasPrefix(call, "time."):
						switch strings.TrimPrefix(call, "time.") {
						case "NewTimer", "NewTicker", "Tick", "After", "AfterFunc", "Sleep":
							kind = "timer"
						}
					case strings.HasPrefix(call, "workpool.New"):
						kind = "pool"
					case call == "exec.Command" || call == "exec.CommandContext":
						kind = "subprocess"
					case call == "http.NewRequest" || call == "http.NewRequestWithContext":
						kind = "http_request"
					}
					if kind == "" {
						parts := strings.Split(call, ".")
						switch parts[len(parts)-1] {
						case "Lock", "RLock", "Unlock", "RUnlock":
							kind = "lock"
						case "Go", "Run", "RunEvery", "Submit", "TrySubmit", "Acquire", "AcquireRequest",
							"Reserve", "ReservePipeline", "ReserveShardNodes":
							kind = "asynchronous_boundary"
						case "Close", "Shutdown", "Stop", "Wait", "Kill", "Start":
							kind = "resource_lifecycle"
						}
					}
					if kind != "" {
						add(value, function, kind, call)
					}
				}
				return true
			})
		}
		for _, declaration := range tree.Decls {
			if function, ok := declaration.(*ast.FuncDecl); ok {
				owner := function.Name.Name
				if function.Recv != nil {
					owner = text(function.Recv.List[0].Type) + "." + owner
				}
				if formatErr != nil {
					return fmt.Errorf("format AST in %s: %w", relative, formatErr)
				}
				inspect(function, owner)
			} else {
				inspect(declaration, "<declaration>")
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan repository: %w", err)
	}
	sort.Slice(sites, func(i, j int) bool {
		a, b := sites[i], sites[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Kind < b.Kind
	})
	counts := make(map[string]int)
	for _, site := range sites {
		counts[site.Kind]++
	}
	payload := struct {
		Version  int            `json:"schema_version"`
		Coverage string         `json:"coverage"`
		Files    int            `json:"go_files"`
		Counts   map[string]int `json:"counts"`
		Sites    []site         `json:"sites"`
	}{1, "Non-test Go files, including platform/build-tag variants; excludes testdata, .git, node_modules, .codex and this auditor. Syntactic index, not proof of reachability, boundedness or balanced cleanup. Method-name matches may include synchronous calls.", fileCount, counts, sites}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode census: %w", err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0600); err != nil {
		return fmt.Errorf("write census: %w", err)
	}
	fmt.Printf("%d files; %d sites; %v\n", fileCount, len(sites), counts)
	return nil
}
