# Native Cloak fixtures

These are synthetic HTTPS fixtures, not publisher captures or production data.
The fixed corpus covers JavaScript/same-origin cookie fetch, redirect, delayed
article, large DOM and a completed challenge page. Quality checks use explicit
anchors and failure classifications; successful short/empty content cannot pass.

Run the origin only inside an isolated test namespace. Provide a short-lived
certificate for `native-fixture.test` and a private test key outside Git. The
fixture hostname must contain a dot because the Go extractor rejects
single-label article hosts:

```sh
python tests/fixtures/native-cloak/origin.py --host 127.0.0.1 --port 443 \
  --cert /test/tls/cert.pem --key /test/tls/key.pem \
  --proof-output /test/output/origin-proof.jsonl
```

Trust that certificate only in the test browser, for example with its exact
SPKI trust exception. Do not add a test CA to production or disable all
certificate verification. The Go egress gateway needs its explicit test mode
and an exact `native-fixture.test` fixture-IP allowlist. Origin credentials,
production profiles and external network access are unnecessary.

Example single fresh C invocation, after the native service is ready:

```sh
python scripts/benchmark_native_cloak.py --variant C \
  --origin https://native-fixture.test --cycles 6 --idle-seconds 5,30,60 \
  --endpoint http://127.0.0.1:8080 --proxy http://127.0.0.1:8081 \
  --cgroup worker=/cg/worker --cgroup cloak=/cg/cloak --cgroup gateway=/cg/gateway \
  --output /test/output/C.json
```

Tokens come only from `EXTRACTOR_TEST_TOKEN` and `EGRESS_TOKEN`. The service's
`EXTRACTOR_NATIVE_READY_SELECTOR` must exactly match the runner's fixture selector;
the runner verifies its hash through health capabilities. The Go controller and
its gateway require distinct cgroups. Paths must identify distinct non-nested
cgroups. Missing
mandatory counters fail the run. Absent kernel `memory.peak` is reported as null,
never zero; sampled maxima are labeled separately.

The active benchmark accepts only the Go controller (`--variant C`). Use fresh
containers for independent repeats. One invocation contains repeated jobs, not
independent browser startups. The load generator/origin is outside the
controller measurement; these results do not establish whole-application
resource savings.

See [the approved plan](../../../docs/ai-plans/2026-10-16-go-cloak-native-worker.html)
and [implementation evidence](../../../docs/go-cloak-native-worker.md).
