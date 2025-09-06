from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

from .utils import ensure_dir, resolve_run_date, newest_form_for_date
from .parse_pdf import parse_form_pdf
from .features import build_features
from .model import predict_probabilities
from .value import find_value_bets
from .io_odds import load_odds_csv

def main() -> int:
    parser = argparse.ArgumentParser(description="Greyhound daily betting engine")
    parser.add_argument("--forms-dir", default="forms", help="Folder containing PDFs")
    parser.add_argument("--date", default="today", help="Run date YYYY-MM-DD or 'today'")
    parser.add_argument("--odds-file", default=None, help="Optional odds CSV path")
    parser.add_argument("--bank", type=float, default=40.0, help="Bank size for staking")
    parser.add_argument("--kelly", type=float, default=0.25, help="Fractional Kelly (0..1)")
    args = parser.parse_args()

    run_date = resolve_run_date(args.date)
    forms_dir = Path(args.forms_dir)
    out_dir = ensure_dir(Path("reports") / run_date.strftime("%Y-%m-%d"))

    # 1) Find PDFs for the date (or recent if filename doesn't have date)
    pdfs = newest_form_for_date(forms_dir, run_date)
    if not pdfs:
        print(f"No PDFs found in '{forms_dir}' for {run_date}")
        return 2

    # 2) Parse each PDF
    parsed_list = []
    for pdf in pdfs:
        df = parse_form_pdf(pdf)
        if not df.empty:
            parsed_list.append(df)

    if not parsed_list:
        print("Parser found no runners.")
        return 2

    parsed = pd.concat(parsed_list, ignore_index=True)

    # 3) Build features and probabilities
    feats = build_features(parsed)
    probs = predict_probabilities(feats)

    # 4) Save probabilities
    probs_out = out_dir / "probabilities.csv"
    probs.to_csv(probs_out, index=False)
    print(f"Saved {probs_out}")

    # 5) Optional value bets if odds file provided
    if args.odds_file:
        try:
            odds_df = load_odds_csv(Path(args.odds_file))
            bets = find_value_bets(probs, odds_df, bank=args.bank, kelly=args.kelly)
            bets_out = out_dir / "bets.csv"
            bets.to_csv(bets_out, index=False)
            print(f"Saved {bets_out}")
        except Exception as e:
            print(f"Skipping bets (odds problem): {e}")

    # 6) Simple summary
    md = out_dir / "summary.md"
    top = (
        probs.sort_values(["race", "prob"], ascending=[True, False])
             .groupby(["track", "race"], as_index=False)
             .first()[["track", "race", "runner", "box", "prob"]]
    )
    with md.open("w", encoding="utf-8") as f:
        f.write(f"# Summary – {run_date}\n\n")
        f.write(f"PDFs: {[p.name for p in pdfs]}\n\n")
        f.write("## Top pick per race\n")
        for _, r in top.iterrows():
            f.write(f"- **{r['track']} R{int(r['race'])}**: Box {int(r['box'])} – {r['runner']} (p={r['prob']:.2f})\n")
    print(f"Saved {md}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
