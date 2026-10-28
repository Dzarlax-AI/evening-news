# Frozen private AI runtime compatibility cases

These synthetic fixtures were captured from the actual Python implementation at
commit `958c3dd6c2270cc67448d0d302fc0b8818110da9` before its transport/accounting
code was replaced. The capture used mocked HTTP responses and mocked database
execute/commit calls. No real provider, source site, database, or Telegram call
was made. All keys, URLs, prompts and results are synthetic.

- `legacy_transport.json`: exact provider request JSON, selected endpoint,
  configured timeout, normalized result or error status/Retry-After, and number
  of usage callbacks for ten text/structured cases.
- `legacy_usage.json`: five direct accounting inputs with configured synthetic
  cost rates and their bound insert values, excluding wall-clock timestamps.

Normalization and accounting are separate stages. A missing provider total
normalizes to prompt + completion + cached tokens. Accounting receives that
normalized total. An explicit zero is preserved in the response, then the
legacy accounting fallback uses prompt + completion, excluding cached tokens.
The direct accounting fixture's missing/null inputs must not be confused with
an unnormalized provider response.

Preserve frozen expected values when modifying the implementation. Add an
explicitly explained new case rather than rewriting a failing expectation.
