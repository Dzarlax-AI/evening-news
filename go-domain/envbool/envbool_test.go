package envbool

import (
	"os"
	"testing"
)

func TestLegacyBooleanValues(t *testing.T) {
	const key = "NEWS_BOOL_TEST_FIXTURE"
	_ = os.Unsetenv(key)
	t.Cleanup(func() { _ = os.Unsetenv(key) })
	if v, err := Read(key, true); err != nil || !v {
		t.Fatal("unset default")
	}
	for _, tc := range []struct {
		value string
		want  bool
		valid bool
	}{{"true", true, true}, {"YES", true, true}, {"1", true, true}, {"t", true, true}, {"false", false, true}, {"False", false, true}, {"0", false, true}, {"no", false, true}, {"off", false, true}, {"f", false, true}, {"", false, false}, {" true ", false, false}, {"invalid", false, false}} {
		t.Setenv(key, tc.value)
		v, err := Read(key, true)
		if (err == nil) != tc.valid || (tc.valid && v != tc.want) {
			t.Fatalf("value %q: %v %v", tc.value, v, err)
		}
	}
}
