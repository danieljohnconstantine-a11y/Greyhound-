"""Configuration settings for Greyhound Analysis"""

# Data directories
DATA_DIR = "data"
OUTPUT_DIR = "outputs"

# Scoring weights for different race types
SPRINT_WEIGHTS = {
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

MIDDLE_WEIGHTS = {
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

LONG_WEIGHTS = {
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
