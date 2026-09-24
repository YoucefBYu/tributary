"""S7: the `retry-topic` policy (ADR-003 D2, follow-up 5; ADR-007 D5).

Three delay mechanisms are compared, because Kafka has no per-record delay:

* ``pause``  - option (a): one ``retry`` topic plus ``tb-retry-not-before``; a record that is not
  due pauses its partition until it is. Head-of-line blocking by construction.
* ``buffer`` - option (a'), not in ADR-003's table: one ``retry`` topic plus ``tb-retry-not-before``;
  a record that is not due is held in memory and delivered when due, and the partition's commit is a
  **watermark** (the lowest offset still held). No head-of-line blocking; costs memory and a larger
  replay window after a crash.
* ``tiers``  - option (b): one topic per delay tier, each with a fixed delay, consumed with
  pause-until-due. Head-of-line blocking only from append-time skew inside one tier.

`s7.model` predicts the head-of-line cost at production tiers (5 s / 1 min / 15 min), which no
cluster run can afford to wait out; `s7.runner` measures the same mechanisms on the 3-broker spike
cluster at scaled-down tiers and checks the model against the measurement.
"""
