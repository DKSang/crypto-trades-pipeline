# Polymarket source and publication contract

Labels: OBSERVED is a dated bounded sample; DOCUMENTED records source documentation or code; INFERRED is interpretation; UNVERIFIED is an open claim. No fresh API/cloud checks were made for this distillation (2026-10-23).

## Discovery and REST

| Finding | Label / scope | Provenance |
|---|---|---|
| Gamma event groups markets; market outcomes map by index to CLOB asset IDs. Keep event ID, market ID, condition ID and asset ID distinct. | OBSERVED 2026-10-21: alignment checked on 3 markets, not the entire source. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2]; `docs/evidence/source/clob_samples.json` |
| `/events/keyset?tag_id=439&closed=false&limit=100` returns paginated mutable snapshots, not CDC. | DOCUMENTED; INFERRED: disappearance means departure from selection, not proven deletion. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.1–2.2] |
| Use `next_cursor` as `after_cursor`; terminal page has no `next_cursor`. Offset rejected; explicit limit ≤100. | OBSERVED 2026-10-21: invalid cursor/offset 422; limit 0 returned default 20, limits 101/1000 clamped to 100 despite documented bounds. | `docs/evidence/source/gamma_errors_log.json`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2] |
| AI tag is wider than AI-model scope; open events contain closed/inactive nested markets. | OBSERVED 2026-10-21: 298 events / 3,377 markets / 6,754 assets; 206 closed nested markets. | `docs/evidence/source/gamma_profile.json`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2] |
| `outcomes`, `clobTokenIds`, `outcomePrices` can be JSON strings inside JSON; asset IDs are decimal strings 72–78 digits. | OBSERVED 2026-10-21; never coerce IDs through float/int64. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2]; [gsd-final:fabric/gamma_logic/selector.py parse_json_list, build_watchlist] |
| Cached snapshot is not transactionally consistent across pages. | OBSERVED 2026-10-21: events cache max-age=300; INFERRED: recheck may see cached state and entities may change between pages. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.2] |
| Best bid=max(bids.price), best ask=min(asks.price); do not trust position. | OBSERVED 2026-10-21: 6 REST books, best levels at end; 36 WS books same order. Order guarantee UNVERIFIED. | `docs/evidence/source/clob_samples.json`; `docs/evidence/source/ws_summary.json` |
| REST midpoint/spread matched hand calculations; `/price?side=BUY` gave bid, not ask. `/last-trade-price` lacked size. | OBSERVED 2026-10-21: 3 market pairs; not a current live-vs-REST comparison. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.3]; `docs/evidence/source/clob_samples.json` |
| `/time` is seconds; book timestamp is string milliseconds. | OBSERVED 2026-10-21; avoid unit confusion. | `docs/evidence/source/clob_samples.json`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.3] |
| Source UI may use last trade instead of midpoint for wide spread (>0.10). | DOCUMENTED source claim, not observed UI verification. Project midpoint remains pure midpoint. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.3 P-CONCEPT-PRICES] |

## WebSocket wire and time

| Finding | Label / scope | Provenance |
|---|---|---|
| Market subscription uses `assets_ids`, `type=market`; PING/PONG are application text. | DOCUMENTED; OBSERVED 2026-10-21: 13 PONG for 13 PING in second sample. | `docs/evidence/source/ws_session_meta_run2.json`; [gsd-final:collector/runner.py SessionRunner] |
| Initial frame is a JSON array of books; later frames can be objects; PONG is non-JSON text. | OBSERVED 2026-10-21: 759 frames, 780 JSON events, 13 PONG; 18 assets, two connections. | `docs/evidence/source/ws_summary.json`; `docs/evidence/source/ws_samples_trimmed.json` |
| `price_change` expands its `price_changes` children, each with its own asset/top-of-book. Frame, event object and logical entry counts differ. | OBSERVED 2026-10-21; DOCUMENTED parser expansion. | `docs/evidence/source/ws_samples_trimmed.json`; [gsd-final:collector/envelope.py]; [gsd-final:fabric/sql/ws_parse.sql] |
| Current subscription sets `custom_feature_enabled=false`; sample used true. Optional best-bid/ask and lifecycle events are not required for KPIs. | DOCUMENTED owner choice; do not assume every sample event is subscribed now. | [gsd-final:.planning/phases/02-design/02-CONTEXT.md D2-08, D2-15]; [gsd-final:collector/runner.py] |
| No verified universal event ID/sequence; repeated content can be distinct observations. | UNVERIFIED source uniqueness/exactly-once; capture IDs support transport retry dedupe only. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4]; [gsd-final:collector/envelope.py] |
| Reconnect returned another 18-book snapshot; it restores state, not missed trades. No replay operation found in inspected contract. | OBSERVED 2026-10-21 snapshot; INFERRED missed-trade non-recovery, not proof no other API exists. | `docs/evidence/source/ws_session_meta_run2.json`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Book timestamp behaves like last state change, not snapshot send time. Source age p95 ~584 s / max 708 s. | OBSERVED 2026-10-21: 36 books; min 1.4 s / p50 7.6 s. This is source age, not ingestion latency. | `docs/evidence/source/ws_extra_output.txt`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Source timestamps can regress; do not order receipt windows by source time. | OBSERVED 2026-10-21: 15 arrival-order timestamp regressions in bounded WS sample; no global order guarantee. | `docs/evidence/source/ws_summary.json`; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Price-change receive minus timestamp: p50 825 ms / p95 907 ms / max 1119 ms. | OBSERVED 2026-10-21; includes unmeasured collector clock offset, network and source delay. Not receipt-to-visible SLO. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.4] |
| Preserve source time/raw time and collector UTC receive time separately; quote age ≠ connection health. | DOCUMENTED implementation: receive-time windows, 20 s heartbeat health rule. | [gsd-final:collector/envelope.py]; [gsd-final:fabric/kpi_logic/reference.py spread_connection_state] |

## Selector `sel-v0.2`

DOCUMENTED current code: normalize Unicode/case/whitespace/hyphens across event title/slug and market question/title/slug; exact AI tag name/slug; validate nested market status, parsed outcome/token lengths. `acceptingOrders` must not be false (missing is not automatically false). [gsd-final:fabric/gamma_logic/selector.py _normalize, _market_valid, select_markets]

| Rule | Current implementation | Provenance |
|---|---|---|
| Include | AI-tagged valid markets: Frontier Math; contextual Riemann/Millennium; or Claude, GPT/ChatGPT, Gemini, Grok, Kimi, DeepSeek, Z.ai/zai/GLM with model/benchmark/release/rank/arena/score context. | [gsd-final:fabric/gamma_logic/selector.py _science_match, _model_match] |
| Deny | Valuation, market cap, IPO, token/output price, OpenRouter market share, acquisition, CEO/stocks, hardware/glasses, social network, outage. Science match exempts deny; simultaneous science+model candidate is ambiguous. | [gsd-final:fabric/gamma_logic/selector.py DENY_TERMS, select_markets] |
| Reasons | `EXCLUDED_INVALID_MARKET`, `EXCLUDED_CRITERIA_DENY`, `EXCLUDED_CRITERIA_NO_MATCH`, `AMBIGUOUS_TITLE`; mapping validation adds `EXCLUDED_INVALID_MARKET_ASSET_MAPPING`. | [gsd-final:fabric/gamma_logic/selector.py select_markets, build_watchlist] |
| No silent override | No per-ID exemptions or volume/liquidity threshold; no measured capacity-derived asset cap. | [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.6]; [gsd-final:fabric/gamma_logic/selector.py] |
| Blind spot | OBSERVED 2026-10-22: 1,675 `AMBIGUOUS_TITLE` “AI Lab” markets excluded; coverage expansion deferred, risk remained unaccepted in that decision. | `docs/evidence/ingest/CLOUD-LOG.md`; [gsd-final:.planning/phases/04-build-transform-kpi/04-CONTEXT.md D4-02] |

DOCUMENTED recheck: selected markets revalidated before subscription; bounded transient retries and deadlines, per-market exclusion reasons; zero valid markets/all transient-exhausted block the session. Exact runtime reason names are in `collector/recheck.py`, not the older proposal's reason table. [gsd-final:collector/recheck.py]; [gsd-final:collector/runner.py SessionRunner]; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.7]

DOCUMENTED implemented recheck reasons: `EXCLUDED_CLOSED`, `EXCLUDED_NOT_ACTIVE`, `EXCLUDED_NOT_ACCEPTING`, `EXCLUDED_NO_ORDERBOOK`, `EXCLUDED_TOKEN_MISMATCH`, `EXCLUDED_RECHECK_INVALID_METADATA`, `EXCLUDED_RECHECK_TIMEOUT`, `EXCLUDED_RECHECK_DEADLINE`, `EXCLUDED_RECHECK_TRANSIENT_EXHAUSTED`. Current token tuple must exactly match pinned asset IDs, not just list length. All-timeout/deadline failures may be labelled zero-valid rather than source-unavailable; no socket still opens. [gsd-final:collector/recheck.py recheck, _check_one]; [gsd-final:.planning/phases/04-build-transform-kpi/04-REREVIEW.md CL-03]

## Metadata publication and fallback

- DOCUMENTED: REST Copy emits one `<RunId>_pages.json`, one JSON response page per line, in `Files/raw/market_intelligence/polymarket/gamma_events/snapshot/YYYY/MM/DD/` (UTC). Supersedes per-page filename proposal only. [gsd-final:.planning/phases/03-build-ingest/03-CONTEXT.md D3-05]; [gsd-final:fabric/gamma_logic/publish.py read_copy_pages]
- DOCUMENTED: require Copy success, page count, valid page/event/market shapes, unique event/market IDs, nonterminal cursors and final cursor absence. Code checks cursor presence/termination, not a proven request-to-response cursor chain or source-wide point-in-time completeness. [gsd-final:fabric/gamma_logic/selector.py completeness_gate]
- DOCUMENTED: immutable version folder contains manifest + watchlist/mapping/exclusions; nonempty valid mapping required. Write both before replacing `published.json` via temporary marker. Manifest records version, publication time, counts, criteria version and hash of sorted unique asset strings. [gsd-final:fabric/gamma_logic/publish.py publish_snapshot]; [gsd-final:fabric/gamma_logic/selector.py watchlist_hash]
- DOCUMENTED: consumer pins last published complete version, records metadata version/watchlist hash, shows age and rechecks. No maximum fallback age; failed/partial run must not replace pointer. Publication time is not source transaction time. [gsd-final:collector/metadata.py]; [gsd-final:.planning/phases/01-inception/01-CONTEXT.md D-14]
- OBSERVED 2026-10-22: successful three-page publication, 214 markets/428 assets. UNVERIFIED: failed-page cloud injection, concurrent publishers and OneLake crash atomicity. [docs/evidence/ingest/CLOUD-LOG.md]; [gsd-final:.planning/phases/03-build-ingest/03-DATA-VERIFY.md V-68/V-69]
- DOCUMENTED warm metadata scope: `published_watchlist`, versioned mapping plus current market/asset rows; omissions soft-delete membership and reappearance reactivates, not physical source deletion. Quote/trade observations have no metadata-style deletion semantics. [gsd-final:fabric/notebooks/nb_stg_to_silver_metadata.py]; [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md DEC-50]
- DOCUMENTED static limit: published watchlist omits question/tick/status/page lineage; active row builder uses `.get`, so absent fields remain null, not verified source defaults. Soft-delete notebook checks complete/nonempty publication but does not compare criteria/scope against previous current state; offline helper scope tests do not prove this adapter protection. [gsd-final:fabric/gamma_logic/selector.py build_watchlist]; [gsd-final:fabric/notebooks/nb_stg_to_silver_metadata.py build_metadata_snapshot_rows, soft delete]; [gsd-final:fabric/silver_logic/soft_delete.py]

## Unverified

Full raw Gamma/WS sample blobs were not retained; summaries/reduced samples cannot reproduce all profiling numbers. First WS sample retains session metadata only; second source sample observed zero trades, while later collected session observed 51. Rate-limit exhaustion, V2 identifier changes, complete source delivery, terms, size units and live REST comparison remain unverified. [gsd-final:.planning/phases/02-design/02-DATA-SPEC.md §2.0–2.5]; `docs/evidence/transform/CLOUD-LOG.md`
