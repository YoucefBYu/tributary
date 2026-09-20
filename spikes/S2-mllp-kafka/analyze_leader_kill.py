"""Re-analyse leader-kill runs from per-message events: longest window with no AA ACK (the
failover gap, an availability cost) and when the ACKs that the old chaos metric flagged landed
relative to the end of that gap. Usage: python analyze_leader_kill.py runs/leader-kill.jsonl"""
import json
import sys

out = []
for line in open(sys.argv[1]):
    r = json.loads(line)
    ev = json.load(open(f"runs/{r['run_id']}.json"))["events"]
    t0 = min(e[1] for e in ev)
    aa = sorted(e[2] for e in ev if e[3] == "AA")
    gaps = [(b - a, a, b) for a, b in zip(aa, aa[1:])]
    g, ga, gb = max(gaps)
    nack_t = [e[2] for e in ev if e[3] != "AA"]
    # AAs for messages sent inside the gap or acked at/after its end but sent after the first NACK
    first_nack_send = min((e[1] for e in ev if e[3] != "AA"), default=None)
    late = [e for e in ev if e[3] == "AA" and first_nack_send and e[1] >= first_nack_send]
    out.append({"trial": r["trial"], "flagged_old_metric": r["aa_while_unavailable"],
                "ack_gap_s": round(g, 2), "gap_start_s": round(ga - t0, 2), "gap_end_s": round(gb - t0, 2),
                "first_nack_s": round(first_nack_send - t0, 2) if first_nack_send else None,
                "last_nack_s": round(max(nack_t) - t0, 2) if nack_t else None,
                "aa_sent_after_first_nack": len(late),
                "min_ack_time_of_those_s": round(min((e[2] for e in late), default=0) - t0, 2)})
    print(json.dumps(out[-1]))
