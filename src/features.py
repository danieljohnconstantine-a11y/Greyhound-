# src/features.py
"""
v3.4 Enhanced Feature Engineering

Features:
- Enhanced Winning Streak (1.30x) - Hot streak dogs boosted
- Closer Bonus for Box 7-8 (+8% at 500m+) - Outside boxes benefit at longer distances
- Trainer Momentum Factor (+12% for recent winners)
- Competitive Field Detection via FieldSimilarityIndex
"""
from typing import List, Dict, Optional
from collections import defaultdict
import statistics

# Box bias prior (heuristic). Sums ≈1 for 8 runners.
# You can tune these numbers later per track.
BOX_PRIOR = [0.17, 0.16, 0.15, 0.13, 0.12, 0.10, 0.09, 0.08]  # index 0 -> box1

# v3.4 Enhancement Constants
WINNING_STREAK_MULTIPLIER = 1.30  # 30% boost for hot streak dogs
CLOSER_BONUS_BOX_7_8 = 0.08       # +8% bonus for boxes 7-8 at 500m+
CLOSER_BONUS_MIN_DISTANCE = 500   # Minimum distance (meters) for closer bonus
TRAINER_MOMENTUM_BOOST = 0.12     # +12% for trainers with recent winners

# Track recent trainer wins for momentum calculation
_trainer_recent_wins: Dict[str, int] = defaultdict(int)


def calculate_winning_streak_boost(runner: Dict) -> float:
    """
    v3.4 Enhanced Winning Streak (1.30x)
    
    Identifies dogs on hot streaks based on recent form figures.
    Dogs with consecutive wins/places get boosted probability.
    
    Returns multiplier (1.0 = no boost, 1.30 = hot streak)
    """
    form = runner.get("form", "") or runner.get("ff", "") or ""
    if not form:
        return 1.0
    
    # Count consecutive wins (1) or places (1,2,3) from most recent
    # Safely handle any characters - only count digits 1-3 as good finishes
    streak_count = 0
    for char in str(form):
        if char in "123":
            streak_count += 1
        elif char.isdigit():
            break  # Streak broken by finishing 4th or worse
        # Skip non-digit characters (spaces, letters, etc.)
    
    # Apply streak multiplier if 2+ consecutive good finishes
    if streak_count >= 3:
        return WINNING_STREAK_MULTIPLIER  # Full 1.30x boost
    elif streak_count == 2:
        return 1.0 + (WINNING_STREAK_MULTIPLIER - 1.0) * 0.5  # Half boost (1.15x)
    
    return 1.0


def calculate_closer_bonus(runner: Dict, distance: Optional[int] = None) -> float:
    """
    v3.4 Closer Bonus for Box 7-8 (+8% at 500m+)
    
    Dogs in outside boxes (7-8) get a bonus at longer distances
    where they can use the extra ground to their advantage.
    
    Returns bonus amount (0.0 to 0.08)
    """
    # Safely parse box value, defaulting to 0 if invalid
    try:
        box = int(runner.get("box", 0))
    except (ValueError, TypeError):
        box = 0
    
    # Validate box is in valid range
    if box < 1 or box > 8:
        return 0.0
    
    # Parse distance from runner data or use provided distance
    if distance is None:
        dist_str = str(runner.get("distance", "0"))
        # Extract numeric part (e.g., "520m" -> 520)
        dist_digits = "".join(c for c in dist_str if c.isdigit())
        distance = int(dist_digits) if dist_digits else 0
    
    # Apply closer bonus only for boxes 7-8 at 500m+
    if box in (7, 8) and distance >= CLOSER_BONUS_MIN_DISTANCE:
        return CLOSER_BONUS_BOX_7_8
    
    return 0.0


def calculate_trainer_momentum(runner: Dict, trainer_wins: Dict[str, int] = None) -> float:
    """
    v3.4 Trainer Momentum Factor (+12% for recent winners)
    
    Trainers with recent winners have demonstrated current form
    and their other runners get a probability boost.
    
    Returns boost amount (0.0 to 0.12)
    """
    trainer = runner.get("trainer", "") or ""
    if not trainer:
        return 0.0
    
    # Use provided trainer wins or global tracker
    wins_dict = trainer_wins if trainer_wins is not None else _trainer_recent_wins
    
    trainer_key = trainer.strip().lower()
    recent_wins = wins_dict.get(trainer_key, 0)
    
    # Apply momentum boost if trainer has recent wins
    if recent_wins >= 2:
        return TRAINER_MOMENTUM_BOOST  # Full +12%
    elif recent_wins == 1:
        return TRAINER_MOMENTUM_BOOST * 0.5  # Half boost +6%
    
    return 0.0


def calculate_field_similarity_index(runners: List[Dict]) -> float:
    """
    v3.4 Competitive Field Detection via FieldSimilarityIndex
    
    Measures how competitive/similar a field is based on form and stats.
    Higher index = more competitive field = harder to predict.
    
    Returns index from 0.0 (one-sided) to 1.0 (very competitive)
    """
    if len(runners) < 2:
        return 0.0
    
    # Calculate based on box priors spread
    # Use dynamic default based on actual field size
    field_size = len(runners)
    default_prior = 1.0 / field_size if field_size > 0 else 0.125
    priors = [r.get("box_prior", default_prior) for r in runners]
    
    if len(priors) < 2:
        return 0.5
    
    try:
        stdev = statistics.stdev(priors)
        mean_prior = statistics.mean(priors)
        
        # Low stdev relative to mean = competitive field
        if mean_prior > 0:
            cv = stdev / mean_prior  # Coefficient of variation
            # Invert so high similarity = high index
            similarity_index = max(0.0, min(1.0, 1.0 - cv))
            return round(similarity_index, 4)
    except statistics.StatisticsError:
        pass
    
    return 0.5


def update_trainer_wins(trainer: str, wins: int = 1) -> None:
    """Update trainer recent wins for momentum tracking."""
    if trainer:
        _trainer_recent_wins[trainer.strip().lower()] += wins


def reset_trainer_wins() -> None:
    """Reset trainer wins tracker (call at start of new day)."""
    _trainer_recent_wins.clear()


def build_features(rows: List[Dict], trainer_wins: Dict[str, int] = None) -> List[Dict]:
    """
    Build enhanced v3.4 features for all runners.
    
    Applies:
    - Box bias priors
    - Enhanced Winning Streak boost
    - Closer Bonus for Box 7-8
    - Trainer Momentum Factor
    - Field Similarity Index
    """
    # Group by (track,date,race) to compute field size and normalize priors
    groups = defaultdict(list)
    for r in rows:
        key = (r["track"], r["date"], r["race"])
        groups[key].append(r)

    feats: list[Dict] = []
    for key, runners in groups.items():
        field_size = len(runners)
        
        # Normalize priors for actual field size
        priors = BOX_PRIOR[:field_size]
        s = sum(priors) if priors else 1.0
        norm_priors = [p / s for p in priors]

        # Get race distance for closer bonus calculation
        race_distance = None
        for r in runners:
            dist_str = str(r.get("distance", "0"))
            dist_digits = "".join(c for c in dist_str if c.isdigit())
            if dist_digits:
                race_distance = int(dist_digits)
                break

        # First pass: calculate base priors and enhancements for each runner
        runner_data = []
        for r in sorted(runners, key=lambda x: x["box"]):
            # Safely parse box with validation
            try:
                box_val = int(r.get("box", 1))
            except (ValueError, TypeError):
                box_val = 1
            box_idx = max(1, min(8, box_val)) - 1
            prior = norm_priors[box_idx] if box_idx < len(norm_priors) else 1.0/field_size
            runner_data.append({"runner": r, "base_prior": prior, "box_idx": box_idx})

        # Calculate field similarity index
        temp_runners = [{"box_prior": rd["base_prior"]} for rd in runner_data]
        field_similarity = calculate_field_similarity_index(temp_runners)

        # Second pass: apply v3.4 enhancements and calculate raw adjusted priors
        adjusted_priors = []
        for rd in runner_data:
            r = rd["runner"]
            base_prior = rd["base_prior"]
            
            # v3.4 Enhanced Winning Streak
            streak_mult = calculate_winning_streak_boost(r)
            
            # v3.4 Closer Bonus for Box 7-8
            closer_bonus = calculate_closer_bonus(r, race_distance)
            
            # v3.4 Trainer Momentum Factor
            trainer_boost = calculate_trainer_momentum(r, trainer_wins)
            
            # Calculate raw adjusted prior (before normalization)
            # Apply streak multiplier first, then add bonuses
            raw_adjusted = (base_prior * streak_mult) + closer_bonus + trainer_boost
            
            rd["streak_mult"] = streak_mult
            rd["closer_bonus"] = closer_bonus
            rd["trainer_boost"] = trainer_boost
            adjusted_priors.append(raw_adjusted)
        
        # Normalize adjusted priors so they sum to 1.0 within the race
        # This maintains probability constraints while preserving relative ordering
        total_adjusted = sum(adjusted_priors) or 1.0
        
        # Third pass: build final feature rows with normalized adjusted priors
        for i, rd in enumerate(runner_data):
            r = rd["runner"]
            base_prior = rd["base_prior"]
            normalized_adjusted = adjusted_priors[i] / total_adjusted
            
            row = dict(r)
            row["field_size"] = field_size
            row["box_prior"] = round(base_prior, 4)
            row["adjusted_prior"] = round(normalized_adjusted, 4)
            row["streak_multiplier"] = rd["streak_mult"]
            row["closer_bonus"] = rd["closer_bonus"]
            row["trainer_momentum"] = rd["trainer_boost"]
            row["field_similarity_index"] = field_similarity
            feats.append(row)
    
    return feats
