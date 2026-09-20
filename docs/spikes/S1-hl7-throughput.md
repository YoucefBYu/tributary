# S1: HL7 parse and transform throughput

Status: DRAFT by qa-engineer, for architect review. Measured on a loaded Windows host (Kafka in Docker and other agents running); repeat-to-repeat spread up to 2x. Verdicts are robust to that, differences under about 2x are not.

## Question

Can a Python pipeline worker parse HL7 v2 and (optionally) transform to a FHIR Bundle at 200 msg/s/core or better? (Plan threshold, phase-0.md S1.)

## Method

- Corpus: `python -m benchmarks.gen --count 10000 --seed 1` (sha256 `a0bdd1c4...4ed4641`), mix ADT A01 30%, A03 20%, A08 20%, ORU R01 20%, ORM O01 10%, mean 465 B, MSH-3 `TRIBUTARY-SYNTH`. Synthetic only.
- Candidates: `python-hl7` (hl7.parse), `hl7apy` (`parse_message`, TOLERANT validation, `find_groups=False`, i.e. its fastest reasonable setting), and a minimal lazy parser (split into segments, split fields on access only).
- Three modes per parser: `parse` (parse only); `parse+extract` (parse plus reading the PID/PV1/OBX fields the transform needs); `parse+extract+fhir` (plus building Patient, Encounter and Observation resources as pydantic v2 models in a Bundle, `model_dump`, `orjson.dumps`). All three parsers produce byte-identical Bundles for the first 1,000 corpus messages (`bench.py --verify`, 0 mismatches for python-hl7 and hl7apy against the lazy reference).
- Single core: in-process loop, per-message `perf_counter_ns`, 100 warmup messages, 3 repeats, median run reported (p50/p99 taken from that run). Multi-process: N spawned processes each run the corpus, synchronized start, total msg/s over wall time.
- Memory: `tracemalloc` retained bytes per parsed message (500 messages held).
- hl7apy is capped at the first 1,000 messages per run (about 12 ms/message; a 10k pass would take minutes per repeat).
- Reproduce: `uv run --no-project --python 3.12 --with hl7apy --with hl7 --with pydantic --with orjson python spikes/S1-hl7-throughput/bench.py --count 10000 --repeats 3 --procs 1,2,4,8,16`. Raw: `benchmarks/results/s1.json` and `s1.log` (git-ignored; regenerate).

## Environment

AMD Ryzen 7 5800H (8 cores / 16 threads), 15.4 GB RAM, Windows 11 (10.0.26220), CPython 3.12 via uv, versions recorded in `s1.json` (hl7apy, hl7, pydantic, orjson). No uvloop (not relevant to parsing). Not an isolated machine: the 3-broker Kafka cluster in Docker and other agents were running during the run.

## Results (single core, median of 3)

| Parser | Mode | msg/s | p50 | p99 |
|---|---|---|---|---|
| lazy | parse | 1,625,171 | 0.4 us | 1.0 us |
| lazy | parse+extract | 106,855 | 8.3 us | 27 us |
| lazy | parse+extract+fhir | 27,669 | 26.6 us | 129 us |
| python-hl7 | parse | 2,356 | 332 us | 1,278 us |
| python-hl7 | parse+extract | 2,319 | 341 us | 1,313 us |
| python-hl7 | parse+extract+fhir | 1,967 | 381 us | 1,838 us |
| hl7apy | parse | 64 | 13.2 ms | 54.5 ms |
| hl7apy | parse+extract | 67 | 11.6 ms | 57.6 ms |
| hl7apy | parse+extract+fhir | 59 | 13.0 ms | 58.3 ms |

Run-to-run spread is large on this shared machine (e.g. lazy parse+extract 85k to 166k; hl7apy 41 to 75 msg/s). The per-run values are in the raw JSON. Confidence: high for orders of magnitude and for the pass/fail verdict per parser; low for any difference under about 2x.

Memory (retained per parsed message, 465 B raw): lazy 0.8 KB (1.7x raw), python-hl7 62 KB (132x), hl7apy 251 KB (533x).

Multi-process, parse+extract+fhir, total msg/s:

| Procs | lazy | python-hl7 | hl7apy |
|---|---|---|---|
| 1 | 11,194 | 1,994 | 55 |
| 2 | 19,542 | 4,062 | 150 |
| 4 | 65,061 | 3,682 | 262 |
| 8 | 105,862 | 8,501 | 384 |
| 16 | 163,263 | 11,359 | 529 |

Scaling is sub-linear and noisy. python-hl7 went from 1,994 (1 process) to 8,501 (8 processes), 4.3x on 8 processes (54% efficiency); lazy 9.5x on 8 was above linear only because its 1-process figure is depressed by noise; hl7apy 7x. Going to 16 processes (SMT threads) added 34% (python-hl7) and 54% (lazy) over 8. python-hl7 at 4 processes (3,682) is below 2 processes (4,062): that is a non-monotonic result I attribute to co-running load (Docker Kafka, other agents) but did not isolate, so treat the scaling curve as indicative only. The 1-process multi-run figures differ from the in-process table for the same reason. Even the worst per-process rate for python-hl7 (about 710 msg/s at 16 processes) stays above the 200 msg/s/core threshold.

## Threshold

Plan threshold: at least 200 msg/s/core for parse plus transform.

| Candidate | parse+extract+fhir msg/s/core | Verdict |
|---|---|---|
| lazy | about 27,700 (138x threshold) | PASS |
| python-hl7 | about 1,970 (9.8x threshold) | PASS |
| hl7apy | about 59 (0.30x threshold, short by 141 msg/s or 3.4x) | FAIL |

Dominant cost: hl7apy builds a full typed object tree with validation (about 250 KB and 12 ms per 465 B message). python-hl7 builds nested Python lists of strings (about 0.3 ms). In the FHIR step the pydantic model construction dominates the lazy path (about 18 us of 27 us).

## Verdict

Threshold met with python-hl7 and with a minimal lazy parser (the lazy parser does no escape-sequence or repetition handling and only light validation, so its 27.7k msg/s is a ceiling, not the figure for a spec-complete parser) at 10x to 138x margin. **S1b (Rust/PyO3 parser) is not needed** for the 200 msg/s/core target. hl7apy fails and should not be used on the hot path; it may still be useful off the hot path (message validation, profile checks) where 12 ms per message and 250 KB per message are acceptable.

## Caveats and follow-ups

- Transform is a small ADT-shaped mapping (Patient, Encounter, Observation), not a full production mapping; real mappings with terminology lookups will cost more. 200 msg/s/core leaves 5 ms per message; python-hl7 already uses 0.5 ms of it and lazy uses 0.04 ms.
- Corpus messages are simple (no repeating groups beyond DG1/OBX, no escapes, no Z-segments, no batch). Real feeds may parse slower with python-hl7. Re-measure on the conformance corpus when available.
- Runs were on Windows with a loaded host; re-run on the Linux CI target for headline numbers.
- Recommendation: lazy, segment-indexed parser in the SDK for the default path (with python-hl7 as fallback if the lazy parser cannot be built to spec), pydantic for FHIR models. Memory of 62 KB per message with python-hl7 matters for batch or claim-check sized messages; the lazy parser avoids it.
- Not measured: Kafka produce or consume cost in the same loop; that belongs to S2.
