package store

import "testing"

func TestSourceFingerprintSharedFixture(t *testing.T) {
	got := SourceFingerprint(42, "Fixture News", "custom", "https://fixture.invalid", true, `{"a": 1, "b": 2}`)
	const want = "d04771bc732ba36e7e623d7138c0bb936cec8de620b4e587c352c67e51229a86"
	if got != want {
		t.Fatalf("fingerprint = %s, want %s", got, want)
	}
	if got == SourceFingerprint(42, "Renamed News", "custom", "https://fixture.invalid", true, `{"a": 1, "b": 2}`) {
		t.Fatal("source name change did not invalidate the fingerprint")
	}
}
