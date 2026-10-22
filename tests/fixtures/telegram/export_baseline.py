"""Regenerate the deterministic Python Article snapshot for the synthetic fixture."""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(0, str(ROOT))

from test_telegram_source_parity import BASELINE, CASES, _python_baseline  # noqa: E402


output = {
    name: _python_baseline(path.read_text(encoding="utf-8"), page_url, source_name)
    for name, (path, page_url, source_name) in CASES.items()
}
BASELINE.write_text(
    json.dumps(output, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
print(f"Wrote {BASELINE.relative_to(ROOT)}")
