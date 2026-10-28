package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestIncidentFilterCommand(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		input string
		want  int
	}{
		{[]string{"--role", "background"}, "sensitive fixture text\n", 0},
		{[]string{"--role", "invalid"}, "sensitive fixture text\n", 2},
		{[]string{"--role", "extractor"}, strings.Repeat("x", 262145), 3},
		{nil, "sensitive fixture text\n", 2},
	} {
		var output bytes.Buffer
		if got := incidentLogFilter(tc.args, strings.NewReader(tc.input), &output); got != tc.want {
			t.Fatalf("exit %d, want %d", got, tc.want)
		}
		if strings.Contains(output.String(), "sensitive fixture text") {
			t.Fatal("unfiltered log escaped")
		}
	}
}
