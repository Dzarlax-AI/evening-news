package digestworkflow

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
)

// Historical fixtures were captured from the existing editorial engine before
// the workflow cutover. Compare the full render and immutable hashes.
func TestFrozenEditorialRender(t *testing.T) {
	requestBytes, err := os.ReadFile("../../go-editorial/testdata/generate-request.json")
	if err != nil {
		t.Fatal(err)
	}
	responseBytes, err := os.ReadFile("../../go-editorial/testdata/generate-response.json")
	if err != nil {
		t.Fatal(err)
	}
	var request editorial.Request
	var response editorial.Response
	if err = json.Unmarshal(requestBytes, &request); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatal(err)
	}
	if response.Edition == nil || response.Rendered == nil {
		t.Fatal("incomplete frozen response")
	}
	if err = editorial.ValidateEdition(request.Snapshot, "ru", *response.Edition); err != nil {
		t.Fatal(err)
	}
	rendered, err := editorial.Render(request.Snapshot, "ru", *response.Edition)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rendered, *response.Rendered) {
		t.Fatal("render drifted from frozen response")
	}
}
