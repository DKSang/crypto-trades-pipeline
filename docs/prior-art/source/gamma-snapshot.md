# Gamma snapshot pipeline

The real V-68 run confirmed that REST Copy writes one UTF-8-with-BOM `setOfObjects` file per run, with one JSON response page per line. The response contains `events` with nested `markets`; continuation pages have `next_cursor`, and the final page omits it. `maxRowsPerFile` and `fileNamePrefix` were ignored.

Each run writes one raw file at `Files/raw/market_intelligence/polymarket/gamma_events/snapshot/YYYY/MM/DD/<RunId>_pages.json`, with one response page per line. The publisher validates each line and the complete page cursor sequence, then publishes only after the Copy activity reports success. The dated path prefix follows D2-19/D2-21; D3-05 records the implemented one-file-per-run layout.
