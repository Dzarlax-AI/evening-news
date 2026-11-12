# Serbian digest recovery verification

Stage A changes preserve completed Russian editions and delivery receipts while treating missing required Serbian output as an explicit failure. Specific reasons cross the private domain boundary and survive in a bounded durable execution result; the existing database error-code constraint remains unchanged. Ambiguous sends, lost prepared receipts and stale fences remain conservative unknown outcomes. Disabled delivery succeeds only when the required Serbian edition exists.

Category creation/update and Serbian activation share the existing operational advisory lock. Enabled Serbian generation requires nonblank category names. The one-shot recovery procedure repairs only missing approved names, preserves existing labels and the 12 historical dispatches, and atomically binds one idempotent job to the frozen edition. Repeated invocation never requeues it.

## Local checks

- PostgreSQL 17.11, disposable loopback database, synthetic data/providers/Telegram only.
- Full `go test -race -count=1 ./...`, `go vet ./...`, `go mod verify`: go-web and go-execution passed.
- Full `go test -race -count=1 -p=1 ./...`, `go vet ./...`, `go mod verify`: go-domain passed. A first run exposed a test-DSN fixture mismatch in the backup fake; it passed with the fixture's expected synthetic password. No product backup change was made.
- After the final runtime guard, focused race tests and vet passed for parent RU completion with incomplete SR, ambiguous RU taking precedence, definitive SR rejection, missing SR, ready/missing SR with delivery disabled, and compatible strict publish-input validation.
- Actual operator SQL executed against migrated isolated schemas: double invocation, active/unknown jobs, existing paired edition, partial/unknown part, completed delivery and unexpected category inventory. Refusals rolled back correction/admission; the successful repeat retained one job and the original dispatch count.
- Authenticated fake domain receipts through the actual Pipeline, execution server, lease/fence and PostgreSQL terminal receipt: missing category and rejected SR failed with bounded reasons; ambiguous sends and missing prepared receipts remained delivery_unknown; confirmed RU plus missing SR failed safely.
- Focused independent review completed without remaining P1/P2 findings. PostgreSQL 16/17 image acceptance and release remain CI gates, not local claims.
- Follow-up review corrections passed race tests and vet in affected packages: the fallback publication uses the selected edition date across midnight, whitespace-only translations retain specific reasons, an existing category gap cannot block stop-delivery/processing controls, and malformed generation markers retain their bounded failure reason. The temporal regression saves SR after the prior-day RU prepare and verifies both fake Telegram deliveries.

Production recovery has not been executed by these tests. APNs implementation and real device acceptance are separate Stage B work.
