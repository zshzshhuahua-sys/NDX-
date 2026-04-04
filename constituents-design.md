# NASDAQ-100 Constituents Fetching Design

Generated: 2026-04-04

## 1. Goal

Replace the hardcoded `NASDAQ_100_SYMBOLS` list in `src/ndx_breadth.py` with a dynamic, cached, and retry-safe constituent fetch pipeline for the NASDAQ-100.

## 2. Data Source Comparison

| Source | Strengths | Weaknesses | Recommended Role |
| --- | --- | --- | --- |
| `NASDAQ Data Link` | Structured data, API key auth, production fit | Requires paid access | Primary if licensed |
| `Wikipedia` | Free, easy to parse | Community-edited, no SLA | Default fallback |
| `Yahoo Finance` | Familiar source | Brittle, no stable API | Validation only |

**Recommendation**: Use provider chain:
1. NASDAQ Data Link (if configured)
2. Wikipedia (default)
3. Yahoo Finance (emergency)
4. Hardcoded list (final fallback)

## 3. Update Strategy

### 3.1 Schedule
- Run at `08:00 America/New_York` (before market open)
- Retry at `08:15 America/New_York`
- Keep previous snapshot if refresh fails

### 3.2 Cache Structure
```
data/cache/constituents/
├── 2026-04-04.json
├── 2026-04-03.json
└── latest.json
```

### 3.3 Validation Rules
- Symbol count: 95-110
- Symbols unique and uppercase
- Major tickers present (AAPL, MSFT, NVDA)

## 4. Implementation

### 4.1 Core Types

```python
@dataclass(frozen=True)
class ConstituentsSnapshot:
    as_of_date: str
    fetched_at: str
    source: str
    symbols: list[str]
```

### 4.2 Provider Chain

```python
providers = [
    ("nasdaq_data_link", self._fetch_from_nasdaq_data_link),
    ("wikipedia", self._fetch_from_wikipedia),
    ("yahoo_finance", self._fetch_from_yahoo_finance),
]
```

### 4.3 Wikipedia Fetcher (Default)

```python
def _fetch_from_wikipedia(self) -> list[str]:
    tables = pd.read_html("https://en.wikipedia.org/wiki/NASDAQ-100")
    for table in tables:
        if "ticker" in columns:
            return table["ticker"].dropna().astype(str).tolist()
```

## 5. Integration

```python
def resolve_nasdaq_100_symbols() -> list[str]:
    fetcher = ConstituentsFetcher(cache_dir=Path("data/cache/constituents"))
    try:
        return fetcher.get_constituents()
    except Exception as exc:
        return NASDAQ_100_SYMBOLS  # Fallback
```

## 6. Failure Policy

1. Today's cache
2. Latest successful cache
3. Hardcoded list
4. Fail job only if none available

## 7. Files to Create

```
ndx-200ma-breadth/
├── src/
│   ├── ndx_breadth.py
│   └── constituents.py        # NEW
├── data/
│   └── cache/
│       └── constituents/      # NEW
└── constituents-design.md
```
