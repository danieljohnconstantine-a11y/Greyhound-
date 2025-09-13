import csv, pathlib, math
import pandas as pd
from .utils import OUT_BASE, utcstamp

IN_DIR = OUT_BASE / "combined"
OUT_DIR = pathlib.Path("reports") / "latest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _sigmoid(x): return 1 / (1 + math.exp(-x))

def main():
    ts = utcstamp()
    latest_csvs = sorted(IN_DIR.glob("full_day_*.csv"))
    if not latest_csvs:
        # create empty report to avoid failing pipeline
        (OUT_DIR/"summary.md").write_text("# Summary — no data\n", encoding="utf-8")
        return

    df = pd.read_csv(latest_csvs[-1])
    if df.empty:
        (OUT_DIR/"summary.md").write_text("# Summary — empty data\n", encoding="utf-8")
        return

    # trainer prior (more runners entered → tiny edge)
    t_prior = (df.groupby("trainer")["runner"].count() / len(df)).rename("trainer_freq")
    # box bias (just counts)
    b_prior = (df.groupby("box")["runner"].count() / len(df)).rename("box_freq")

    dd = df.join(t_prior, on="trainer").join(b_prior, on="box").fillna(0)
    # linear score then mapped to (0,1)
    dd["score"] = dd["trainer_freq"] * 1.2 + dd["box_freq"] * 0.8
    dd["prob_win"] = _sigmoid((dd["score"] - dd["score"].mean()) / (dd["score"].std() + 1e-6))

    # Normalize probabilities within each race
    dd["key"] = dd["track"].astype(str) + "|" + dd["date"].astype(str) + "|R" + dd["race"].astype(str)
    dd["prob_win"] = dd.groupby("key")["prob_win"].transform(lambda s: s / s.sum())

    # Save probabilities
    probs_csv = OUT_DIR / "probabilities.csv"
    dd[["track","date","race","box","runner","prob_win"]].to_csv(probs_csv, index=False)

    # Simple picks (top box per race)
    picks = (
        dd.sort_values(["key","prob_win"], ascending=[True,False])
          .groupby("key")
          .head(1)
    )

    # Write summary
    lines = [f"# Summary — {ts[:10]}", ""]
    for _, r in picks.iterrows():
        lines.append(f"- **{r['track']} R{int(r['race'])}** → Box **{int(r['box'])}** — {r['runner']} (p≈{r['prob_win']:.2f})")
    (OUT_DIR/"summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()
