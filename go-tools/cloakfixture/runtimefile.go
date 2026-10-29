package cloakfixture

import (
	"encoding/json"
	"errors"
	"os"
)

type runtimeManifest struct {
	Version        int               `json:"version"`
	Service        string            `json:"service"`
	InstallationID string            `json:"installation_id"`
	Values         map[string]string `json:"values"`
}

// writeRuntimeManifest creates a private role-scoped fixture file. Callers must
// keep the containing directory private and must never include values in logs.
func writeRuntimeManifest(path, service string, values map[string]string) error {
	if path == "" || service == "" || len(values) == 0 {
		return errors.New("runtime_manifest_invalid")
	}
	data, err := json.Marshal(runtimeManifest{Version: 1, Service: service, InstallationID: "native-cloak-fixture", Values: values})
	if err != nil {
		return errors.New("runtime_manifest_invalid")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("runtime_manifest_write_failed")
	}
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return errors.New("runtime_manifest_write_failed")
	}
	return nil
}
