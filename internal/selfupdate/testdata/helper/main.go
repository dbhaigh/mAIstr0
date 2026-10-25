package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/maistr0/maistr0/internal/selfupdate"
)

var release = "9.0.0"

func main() {
	show := flag.Bool("version", false, "version")
	job := flag.String("apply-peer-update", "", "job")
	replace := flag.String("replace", "", "staged")
	record := flag.String("record", "", "output")
	cleanup := flag.String("cleanup-peer-update", "", "cleanup")
	flag.Parse()
	if *cleanup != "" {
		if err := selfupdate.Cleanup(*cleanup); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if *show {
		fmt.Println("maistr0 " + release)
		return
	}
	if *job != "" {
		if err := selfupdate.Apply(*job); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *replace != "" {
		target, _ := os.Executable()
		manifest, err := selfupdate.Inspect(*replace, "9.0.1")
		if err == nil {
			err = selfupdate.LaunchHelper(target, *replace, manifest, []string{"-record", *record})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	directory, _ := os.Getwd()
	data, _ := json.Marshal(struct {
		Version, Directory string
		Args               []string
	}{release, directory, selfupdate.RestartArgs(os.Args[1:])})
	if err := os.WriteFile(*record, data, 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
