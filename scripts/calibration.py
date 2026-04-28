"""calibration.py — Brier / ECE for the weather model.

Reconciles predictions.jsonl (one record per weather intent fired) against
outcomes.jsonl (one record per settled position). Reports overall Brier
score, expected calibration error (ECE), and a 10-bin reliability table
split by signal source (METAR / NWS / NWS+GEFS / etc).

Outcome of a YES bet:
  pnl > 0  → YES won  → outcome = 1
  pnl < 0  → YES lost → outcome = 0
For NO bets, invert. Break-evens are dropped.

Usage:
    python scripts/calibration.py
    python scripts/calibration.py --predictions predictions.jsonl --outcomes outcomes.jsonl
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def load_rotated(directory: Path, prefix: str) -> list[dict]:
    """Load all `<prefix>-YYYY-MM-DD.jsonl` plus legacy `<prefix>.jsonl`."""
    out: list[dict] = []
    for p in sorted(directory.glob(f"{prefix}-*.jsonl")):
        out.extend(load_jsonl(p))
    legacy = directory / f"{prefix}.jsonl"
    if legacy.exists():
        out.extend(load_jsonl(legacy))
    return out


def reconcile(predictions: list[dict], outcomes: list[dict]) -> list[tuple[float, int, str]]:
    """Return list of (predicted_prob_for_side, won, source)."""
    out_by_ticker = defaultdict(list)
    for o in outcomes:
        out_by_ticker[o["ticker"]].append(o)

    matched = []
    for p in predictions:
        side = p.get("side")
        prob = p.get("fair_prob")
        source = p.get("source", "")
        if side is None or prob is None:
            continue
        candidates = out_by_ticker.get(p["ticker"], [])
        if not candidates:
            continue
        # Pick the outcome whose ts is after the prediction's ts.
        match = next((c for c in candidates if c.get("ts", "") >= p.get("ts", "")), None)
        if match is None:
            continue
        pnl = match.get("pnl", 0.0)
        if abs(pnl) < 0.01:
            continue
        won = 1 if pnl > 0 else 0
        # `prob` is fair_prob for YES. If we bet NO, the probability of
        # OUR side winning is 1 - fair_prob.
        side_prob = prob if side == "yes" else (1.0 - prob)
        matched.append((side_prob, won, source))
    return matched


def brier(pairs: list[tuple[float, int, str]]) -> float:
    if not pairs:
        return float("nan")
    return sum((p - y) ** 2 for p, y, _ in pairs) / len(pairs)


def ece(pairs: list[tuple[float, int, str]], n_bins: int = 10) -> tuple[float, list[dict]]:
    if not pairs:
        return float("nan"), []
    bins = [[] for _ in range(n_bins)]
    for p, y, _ in pairs:
        idx = min(int(p * n_bins), n_bins - 1)
        bins[idx].append((p, y))
    total = len(pairs)
    err = 0.0
    table = []
    for i, b in enumerate(bins):
        if not b:
            table.append({"bin": f"{i/n_bins:.1f}-{(i+1)/n_bins:.1f}", "n": 0})
            continue
        avg_p = sum(p for p, _ in b) / len(b)
        avg_y = sum(y for _, y in b) / len(b)
        err += (len(b) / total) * abs(avg_p - avg_y)
        table.append({
            "bin": f"{i/n_bins:.1f}-{(i+1)/n_bins:.1f}",
            "n": len(b),
            "avg_pred": round(avg_p, 3),
            "avg_actual": round(avg_y, 3),
            "gap": round(avg_p - avg_y, 3),
        })
    return err, table


def summarize(directory: str = ".") -> Optional[dict]:
    """Programmatic entry point — used by bot.py at startup to log a
    one-line calibration summary. Returns None if no pairs reconciled.
    """
    base = Path(directory)
    predictions = load_rotated(base, "predictions")
    outcomes = load_rotated(base, "outcomes")
    pairs = reconcile(predictions, outcomes)
    if not pairs:
        return None
    e, _ = ece(pairs)
    return {"n": len(pairs), "brier": brier(pairs), "ece": e}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".", help="directory with rotated *.jsonl files")
    args = ap.parse_args()

    base = Path(args.dir)
    predictions = load_rotated(base, "predictions")
    outcomes = load_rotated(base, "outcomes")
    print(f"Loaded {len(predictions)} predictions, {len(outcomes)} outcomes")

    pairs = reconcile(predictions, outcomes)
    print(f"Reconciled {len(pairs)} prediction/outcome pairs\n")
    if not pairs:
        return

    print(f"Overall Brier:  {brier(pairs):.4f}  (lower=better, 0.25=coin flip)")
    e, table = ece(pairs)
    print(f"Overall ECE:    {e:.4f}\n")

    print("Reliability table (overall):")
    print(f"  {'bin':<10} {'n':>5} {'pred':>7} {'actual':>7} {'gap':>7}")
    for row in table:
        if row["n"] == 0:
            continue
        print(f"  {row['bin']:<10} {row['n']:>5} {row['avg_pred']:>7.3f} {row['avg_actual']:>7.3f} {row['gap']:>+7.3f}")

    by_source = defaultdict(list)
    for p, y, s in pairs:
        by_source[s or "?"].append((p, y, s))
    print("\nBy source:")
    for src, sp in by_source.items():
        e_src, _ = ece(sp)
        print(f"  {src:<20} n={len(sp):>4}  brier={brier(sp):.4f}  ece={e_src:.4f}")


if __name__ == "__main__":
    main()
