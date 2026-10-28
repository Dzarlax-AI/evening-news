// Package incidentfilter filters remote container logs before SSH transport.
package incidentfilter

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Filter streams bounded Docker log output as allowlisted records only. A caller
// must also check the producer exit status before advancing its log cursor.
// A log_overflow error must result in exit code 3; other errors use exit code 2.
func Filter(input io.Reader, output io.Writer, role string) error {
	if role != "background" && role != "extractor" {
		return errors.New("log_unavailable")
	}
	r := bufio.NewReader(io.LimitReader(input, 262145))
	total, lines := 0, 0
	for {
		line, err := r.ReadString('\n')
		total += len(line)
		if total > 262144 {
			return errors.New("log_overflow")
		}
		if len(line) > 0 {
			lines++
			if lines > 64 {
				return errors.New("log_overflow")
			}
			line = strings.TrimSuffix(line, "\n")
			if len(line) > 8192 {
				return errors.New("log_overflow")
			}
			{
				e := ParseLogLine(line)
				if e != nil {
					e["source"] = role
					b, _ := json.Marshal(e)
					if _, writeErr := fmt.Fprintf(output, "log=%s\n", b); writeErr != nil {
						return errors.New("log_unavailable")
					}
				}
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.New("log_unavailable")
		}
	}
}
