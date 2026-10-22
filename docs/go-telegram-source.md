# Offline Telegram channel parser

`extractor.ParseTelegramChannel(html, pageURL, sourceName, limit, fallbackTime)`
parses a captured public-channel page into ordered `TelegramArticle` values.
It performs no network, browser, database, AI, or delivery work. It is not yet
connected to the production Telegram source or exposed through the extractor API.

The result mirrors Python `sources.base.Article`, including its `raw_data`.
An external article can remain the primary `url`; `telegram_url` and `message_id`
preserve the original Telegram identity separately. Text, media order, nullable
metadata and UTC dates are checked against the current Python parser.

The DOM is parsed once. `limit` counts accepted posts; short posts without media
are skipped. The existing extraction ceilings are retained: 5 MiB input and
100,000 characters per post. Limit violations return explicit errors rather than
silently truncating content. A zero fallback time is invalid, and the parser
never reads the wall clock. Malformed-but-recoverable markup follows the HTML
parser's recovery behavior; missing messages return `ErrTelegramEmpty`.

## Offline compatibility checks

The shared synthetic fixtures and Python-generated snapshot live in
`tests/fixtures/telegram/`. The Python test checks the snapshot against the real
Python parser; the Go test independently checks its output against that snapshot.
The Python oracle freezes only the clock and stubs external-article enrichment.
Real post URLs and IDs are compared without normalization. Generated
poll/contact/location fragment hash suffixes are normalized because Python's
`hash()` changes between processes; Go generates stable fragments. UTC timestamp
serialization is normalized while retaining Python's microsecond precision.

```sh
NO_BROWSER=1 .venv/bin/python -m pytest tests/test_telegram_source_parity.py tests/test_telegram_extraction.py -q
cd go
go test ./extractor -count=1
go test -race ./extractor
```

After intentional Python behavior changes, regenerate and review the snapshot:

```sh
PYTHONPATH=. .venv/bin/python tests/fixtures/telegram/export_baseline.py
```

Production fetching, browser fallback and the Python caller adapter are a later
integration step. Existing `extractTelegram` replay behavior remains unchanged.
