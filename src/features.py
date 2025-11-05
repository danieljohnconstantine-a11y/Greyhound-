import pandas as pd
import numpy as np

def compute_features(df):
    df = df.copy()

    # Ensure numeric types
    df["DLR"] = pd.to_numeric(df["DLR"], errors="coerce")
    df["CareerStarts"] = pd.to_numeric(df["CareerStarts"], errors="coerce")
    df["Distance"] = pd.to_numeric(df["Distance"], errors="coerce")

    # Placeholder values — replace with parsed metrics later
    df["BestTimeSec"] = 22.5
    df["SectionalSec"] = 8.5
    df["Last3TimesSec"] = [[22.65, 22.52, 22.77]] * len(df)
    df["Margins"] = [[5.0, 6.3, 10.3]] * len(df)
    df["BoxBiasFactor"] = 0.1
    df["TrackConditionAdj"] = 1.0

    # Derived metrics
    df["Speed_kmh"] = (df["Distance"] / df["BestTimeSec"]) * 3.6
    df["EarlySpeedIndex"] = df["Distance"] / df["SectionalSec"]
    df["FinishConsistency"] = df["Last3TimesSec"].apply(lambda x: np.std(x))
    df["MarginAvg"] = df["Margins"].apply(lambda x: np.mean(x))
    df["FormMomentum"] = df["Margins"].apply(lambda x: np.mean(np.diff(x)) if len(x) >= 2 else 0)

    # Consistency Index
    df["ConsistencyIndex"] = df.apply(
        lambda row: row["CareerWins"] / row["CareerStarts"] if row["CareerStarts"] > 0 else 0,
        axis=1
    )

    # Recent Form Boost
    df["RecentFormBoost"] = df.apply(
        lambda row: 1.0 if row["DLR"] <= 5 and row["CareerWins"] > 0 else 0.5 if row["DLR"] <= 10 else 0,
        axis=1
    )

    # Distance Suitability
    df["DistanceSuit"] = df["Distance"].apply(lambda x: 1.0 if x in [515, 595] else 0.7)

    # Fallbacks
    df["TrainerStrikeRate"] = df.get("TrainerStrikeRate", pd.Series([0.15] * len(df)))
    df["RestFactor"] = df.get("RestFactor", pd.Series([0.8] * len(df)))

    # Overexposure Penalty
    df["OverexposedPenalty"] = df["CareerStarts"].apply(lambda x: -0.1 if x > 80 else 0)

    # Race-type adaptive weighting
    def get_weights(distance):
        if distance < 400:  # Sprint
            return {
                "EarlySpeedIndex": 0.30,
                "Speed_kmh": 0.20,
                "ConsistencyIndex": 0.10,
                "FinishConsistency": 0.05,
                "PrizeMoney": 0.10,
                "RecentFormBoost": 0.10,
                "BoxBiasFactor": 0.10,
                "TrainerStrikeRate": 0.05,
                "DistanceSuit": 0.05,
                "TrackConditionAdj": 0.05
            }
        elif distance <= 500:  # Middle
            return {
                "EarlySpeedIndex": 0.25,
                "Speed_kmh": 0.20,
                "ConsistencyIndex": 0.15,
                "FinishConsistency": 0.05,
                "PrizeMoney": 0.10,
                "RecentFormBoost": 0.10,
                "BoxBiasFactor": 0.05,
                "TrainerStrikeRate": 0.05,
                "DistanceSuit": 0.05,
                "TrackConditionAdj": 0.05
            }
        else:  # Long
            return {
                "EarlySpeedIndex": 0.20,
                "Speed_kmh": 0.15,
                "ConsistencyIndex": 0.20,
                "FinishConsistency": 0.10,
                "PrizeMoney": 0.10,
                "RecentFormBoost": 0.10,
                "BoxBiasFactor": 0.05,
                "TrainerStrikeRate": 0.05,
                "DistanceSuit": 0.05,
                "TrackConditionAdj": 0.05
            }

    # FinalScore calculation
    final_scores = []
    for _, row in df.iterrows():
        w = get_weights(row["Distance"])
        score = (
            row["EarlySpeedIndex"] * w["EarlySpeedIndex"] +
            row["Speed_kmh"] * w["Speed_kmh"] +
            row["ConsistencyIndex"] * w["ConsistencyIndex"] +
            row["FinishConsistency"] * w["FinishConsistency"] +
            (row["PrizeMoney"] / 1000) * w["PrizeMoney"] +
            row["RecentFormBoost"] * w["RecentFormBoost"] +
            row["BoxBiasFactor"] * w["BoxBiasFactor"] +
            row["TrainerStrikeRate"] * w["TrainerStrikeRate"] +
            row["DistanceSuit"] * w["DistanceSuit"] +
            row["TrackConditionAdj"] * w["TrackConditionAdj"] +
            row["OverexposedPenalty"]
        )
        final_scores.append(score)

    df["FinalScore"] = final_scores
    return df

def generate_trifecta_table(df):
    trifecta_rows = []

    for (track, race), group in df.groupby(["Track", "RaceNumber"]):
        top3 = group.sort_values("FinalScore", ascending=False).head(3)
        if len(top3) < 3:
            continue

        scores = top3["FinalScore"].values
        separation_score = (scores[0] - scores[1]) + (scores[1] - scores[2])

        # Confidence tiering
        if scores[0] > 42 and separation_score > 3:
            tier = "Tier 1"
        elif scores[0] > 40 and separation_score > 2:
            tier = "Tier 2"
        elif scores[0] > 38 and separation_score > 1.5:
            tier = "Tier 3"
        else:
            tier = "Tier 4"

        trifecta_rows.append({
            "Track": track,
            "RaceNumber": race,
            "Dog1": top3.iloc[0]["DogName"],
            "Dog2": top3.iloc[1]["DogName"],
            "Dog3": top3.iloc[2]["DogName"],
            "Score1": scores[0],
            "Score2": scores[1],
            "Score3": scores[2],
            "SeparationScore": round(separation_score, 3),
            "ConfidenceTier": tier,
            "BetFlag": "BET" if tier in ["Tier 1", "Tier 2"] else "NO BET"
        })

    trifecta_df = pd.DataFrame(trifecta_rows)
    trifecta_df = trifecta_df.sort_values("SeparationScore", ascending=False)
    return trifecta_df
