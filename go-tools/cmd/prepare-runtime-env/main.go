// prepare-runtime-env renders Compose into a private, reviewable candidate.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "runtime environment preparation failed ("+err.Error()+")")
		os.Exit(1)
	}
	fmt.Println("Runtime environment candidate prepared; private outputs are mode 0600")
}
func run() error {
	compose := flag.String("compose", "", "existing production Compose file")
	output := flag.String("output-dir", "", "new private output directory")
	project := flag.String("project", "", "Compose project name")
	source := flag.String("source-env", "", "source interpolation file")
	flag.Parse()
	if *compose == "" || *output == "" || *project == "" {
		return errors.New("missing_required_argument")
	}
	path, e := filepath.Abs(*compose)
	if e != nil {
		return errors.New("compose_file_not_found")
	}
	if fi, e := os.Stat(path); e != nil || !fi.Mode().IsRegular() {
		return errors.New("compose_file_not_found")
	}
	path, e = filepath.EvalSymlinks(path)
	if e != nil {
		return errors.New("compose_file_not_found")
	}
	envPath := ""
	if *source != "" {
		envPath, e = filepath.Abs(*source)
		if e != nil {
			return errors.New("source_interpolation_file_not_found")
		}
		if fi, e := os.Stat(envPath); e != nil || !fi.Mode().IsRegular() {
			return errors.New("source_interpolation_file_not_found")
		}
		envPath, e = filepath.EvalSymlinks(envPath)
		if e != nil {
			return errors.New("source_interpolation_file_not_found")
		}
	} else {
		p := filepath.Join(filepath.Dir(path), ".env")
		if fi, e := os.Stat(p); e == nil && fi.Mode().IsRegular() {
			envPath, e = filepath.EvalSymlinks(p)
			if e != nil {
				return errors.New("source_interpolation_file_not_found")
			}
		}
	}
	renderEnv := envPath
	if renderEnv == "" {
		renderEnv = os.DevNull
	}
	cmd := exec.Command("docker", "compose", "--project-name", *project, "--env-file", renderEnv, "-f", path, "config", "--format", "json")
	cmd.Dir = filepath.Dir(path)
	cmd.Env = []string{}
	for _, key := range []string{"PATH", "HOME", "DOCKER_CONFIG"} {
		if value, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+value)
		}
	}
	raw, e := cmd.Output()
	if e != nil {
		var ex *exec.Error
		if errors.As(e, &ex) {
			return errors.New("compose_command_unavailable")
		}
		return errors.New("compose_render_failed")
	}
	candidate, dotenv, audit, e := runtimeenv.Prepare(raw, *project)
	if e != nil {
		return e
	}
	audit.SourceCompose = path
	if envPath != "" {
		audit.SourceEnv = &envPath
	}
	auditBytes, e := json.MarshalIndent(audit, "", "  ")
	if e != nil {
		return errors.New("output_write_failed")
	}
	var auditMap map[string]any
	if json.Unmarshal(auditBytes, &auditMap) != nil {
		return errors.New("output_write_failed")
	}
	if envPath == "" {
		auditMap["source_interpolation_file"] = nil
	}
	auditBytes, e = json.MarshalIndent(auditMap, "", "  ")
	if e != nil {
		return errors.New("output_write_failed")
	}
	candidateBytes, e := json.MarshalIndent(candidate, "", "  ")
	if e != nil {
		return errors.New("output_write_failed")
	}
	if _, e = os.Lstat(*output); e == nil {
		return errors.New("output_directory_exists")
	} else if !os.IsNotExist(e) {
		return errors.New("output_write_failed")
	}
	if e = os.MkdirAll(filepath.Dir(*output), 0700); e != nil {
		return errors.New("output_write_failed")
	}
	if e = os.Mkdir(*output, 0700); e != nil {
		return errors.New("output_write_failed")
	}
	for _, f := range []struct {
		name string
		data []byte
	}{{"compose.candidate.json", append(candidateBytes, '\n')}, {".env", []byte(dotenv)}, {"audit.json", append(auditBytes, '\n')}} {
		p := filepath.Join(*output, f.name)
		out, e := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return errors.New("output_write_failed")
		}
		_, we := out.Write(f.data)
		ce := out.Close()
		if we != nil || ce != nil {
			return errors.New("output_write_failed")
		}
	}
	return nil
}
