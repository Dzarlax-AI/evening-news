package configcatalog

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// Explicit opt-in to print discovery for catalog maintenance; never rewrites it.
func TestPrintDiscovery(t *testing.T) {
	if os.Getenv("CONFIG_CATALOG_DISCOVER") != "1" {
		t.Skip("discovery disabled")
	}
	m, e := discover("../..")
	if e != nil {
		t.Fatal(e)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b, _ := json.Marshal(m[k])
		fmt.Printf("%s\t%s\n", k, strings.TrimSpace(string(b)))
	}
}
