# SmartFilter v1 frozen reference corpus

Each JSON file records the input and exact `(should_process, reason)` output from the legacy Python `SmartFilter.should_process_with_ai` implementation. The reference run used only synthetic text, disabled database duplicate checks, and made no external calls. Run Go parity tests from `go-processing` with `go test ./internal/processing`.
