# src/features.py
"""
v3.5 Enhanced Feature Engineering

Features:
- Enhanced Winning Streak (1.40x) - Hot streak dogs boosted with win weighting
- Closer Bonus for Box 7-8 (+8% at 500m+) - Outside boxes benefit at longer distances
- Railer Bonus for Box 1-2 (+6% at <400m) - Inside boxes benefit at short distances
- Trainer Momentum Factor (+6% for 3+ recent wins) - Reduced and more selective
- Competitive Field Detection via FieldSimilarityIndex with adaptive confidence
- Track-Specific Box Priors - Per-track tuning based on historical patterns
- Speed/Time Factor - Best time comparison for quality assessment
"""
from typing import List, Dict, Optional
from collections import defaultdict
import statistics

# Default box bias prior (heuristic). Sums ≈1 for 8 runners.
BOX_PRIOR = [0.17, 0.16, 0.15, 0.13, 0.12, 0.10, 0.09, 0.08]  # index 0 -> box1

# Suggestion 1: Track-specific box priors based on historical patterns
# Tracks where inside boxes (1-2) perform better
TRACK_BOX_PRIORS = {
    # Tracks with strong inside bias (tight turns, short distances common)
    "gawler": [0.20, 0.18, 0.14, 0.12, 0.10, 0.10, 0.08, 0.08],
    "goulburn": [0.19, 0.17, 0.15, 0.13, 0.11, 0.10, 0.08, 0.07],
    "wagga": [0.19, 0.17, 0.15, 0.12, 0.11, 0.10, 0.08, 0.08],
    # Tracks where outside boxes have advantage (wider tracks, longer distances)
    "mandurah": [0.14, 0.13, 0.12, 0.12, 0.12, 0.12, 0.12, 0.13],
    "meadows": [0.15, 0.14, 0.13, 0.12, 0.11, 0.11, 0.12, 0.12],
    "warragul": [0.15, 0.14, 0.13, 0.12, 0.12, 0.11, 0.11, 0.12],
    "healesville": [0.15, 0.14, 0.13, 0.13, 0.12, 0.11, 0.11, 0.11],
    # Balanced tracks
    "bendigo": [0.16, 0.15, 0.14, 0.13, 0.12, 0.11, 0.10, 0.09],
    "richmond": [0.16, 0.15, 0.14, 0.13, 0.12, 0.11, 0.10, 0.09],
}

# v3.5 Enhancement Constants (Updated from v3.4)
# Suggestion 3: Increase Winning Streak emphasis
WINNING_STREAK_MULTIPLIER = 1.40  # Increased from 1.30 to 1.40 (40% boost)
WIN_BONUS = 0.10                  # Extra 10% for actual wins (1st) vs places (2nd/3rd)

# Closer and Railer bonuses
CLOSER_BONUS_BOX_7_8 = 0.08       # +8% bonus for boxes 7-8 at 500m+
CLOSER_BONUS_MIN_DISTANCE = 500   # Minimum distance (meters) for closer bonus

# Suggestion 5: Add Railer bonus for inside boxes at short distances
RAILER_BONUS_BOX_1_2 = 0.06       # +6% bonus for boxes 1-2 at <400m
RAILER_BONUS_MAX_DISTANCE = 400   # Maximum distance (meters) for railer bonus

# Suggestion 2: Reduce Trainer Momentum (was too aggressive)
TRAINER_MOMENTUM_BOOST = 0.06     # Reduced from 0.12 to 0.06 (+6% for recent winners)
TRAINER_MOMENTUM_MIN_WINS = 3     # Require 3+ wins for full boost (was 2)

# Suggestion 6: FSI adaptive threshold
FSI_HIGH_COMPETITION_THRESHOLD = 0.80  # Above this = very competitive, reduce confidence

# Track recent trainer wins for momentum calculation
_trainer_recent_wins: Dict[str, int] = defaultdict(int)


def calculate_winning_streak_boost(runner: Dict) -> float:
    """
    v3.5 Enhanced Winning Streak (1.40x) with Win Weighting
    
    Identifies dogs on hot streaks based on recent form figures.
    Dogs with consecutive wins/places get boosted probability.
    Actual wins (1st) weighted higher than places (2nd/3rd).
    
    Returns multiplier (1.0 = no boost, up to 1.50 = hot streak with wins)
    """
    form = runner.get("form", "") or runner.get("ff", "") or ""
    if not form:
        return 1.0
    
    # Count consecutive wins (1) or places (1,2,3) from most recent
    # Track actual wins vs places for weighting
    streak_count = 0
    win_count = 0  # Count of 1st place finishes
    
    for char in str(form):
        if char == "1":
            streak_count += 1
            win_count += 1
        elif char in "23":
            streak_count += 1
        elif char.isdigit():
            break  # Streak broken by finishing 4th or worse
        # Skip non-digit characters (spaces, letters, etc.)
    
    # Apply streak multiplier if 2+ consecutive good finishes
    if streak_count >= 3:
        base_mult = WINNING_STREAK_MULTIPLIER  # 1.40x boost
        # Suggestion 3: Weight wins higher - add extra bonus for wins
        win_bonus = (win_count / streak_count) * WIN_BONUS if streak_count > 0 else 0
        return base_mult + win_bonus  # Up to 1.50x for all wins
    elif streak_count == 2:
        base_mult = 1.0 + (WINNING_STREAK_MULTIPLIER - 1.0) * 0.5  # 1.20x
        win_bonus = (win_count / streak_count) * WIN_BONUS * 0.5 if streak_count > 0 else 0
        return base_mult + win_bonus
    
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


def calculate_railer_bonus(runner: Dict, distance: Optional[int] = None) -> float:
    """
    v3.5 NEW: Railer Bonus for Box 1-2 (+6% at <400m)
    
    Dogs in inside boxes (1-2) get a bonus at shorter distances
    where the rail provides an advantage on tight turns.
    
    Returns bonus amount (0.0 to 0.06)
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
        # Extract numeric part (e.g., "390m" -> 390)
        dist_digits = "".join(c for c in dist_str if c.isdigit())
        distance = int(dist_digits) if dist_digits else 0
    
    # Apply railer bonus only for boxes 1-2 at <400m
    if box in (1, 2) and distance > 0 and distance < RAILER_BONUS_MAX_DISTANCE:
        return RAILER_BONUS_BOX_1_2
    
    return 0.0


def calculate_trainer_momentum(runner: Dict, trainer_wins: Dict[str, int] = None) -> float:
    """
    v3.5 Trainer Momentum Factor (+6% for 3+ recent winners)
    
    Reduced from v3.4 to be more selective. Trainers with 3+ recent 
    winners get momentum boost. Requires stronger evidence of form.
    
    Returns boost amount (0.0 to 0.06)
    """
    trainer = runner.get("trainer", "") or ""
    if not trainer:
        return 0.0
    
    # Use provided trainer wins or global tracker
    wins_dict = trainer_wins if trainer_wins is not None else _trainer_recent_wins
    
    trainer_key = trainer.strip().lower()
    recent_wins = wins_dict.get(trainer_key, 0)
    
    # Suggestion 2: Require 3+ wins for full boost (more selective)
    if recent_wins >= TRAINER_MOMENTUM_MIN_WINS:  # 3+ wins
        return TRAINER_MOMENTUM_BOOST  # Full +6%
    elif recent_wins == 2:
        return TRAINER_MOMENTUM_BOOST * 0.5  # Half boost +3%
    
    return 0.0


def calculate_field_similarity_index(runners: List[Dict]) -> float:
    """
    v3.5 Competitive Field Detection via FieldSimilarityIndex
    
    Measures how competitive/similar a field is based on form and stats.
    Higher index = more competitive field = harder to predict.
    
    v3.5: Used adaptively - when FSI > 0.80, reduce confidence multiplier
    
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


def get_fsi_confidence_multiplier(fsi: float) -> float:
    """
    v3.5 NEW: Adaptive confidence based on Field Similarity Index
    
    When FSI is very high (competitive field), reduce confidence in picks.
    This helps avoid overconfident bets in unpredictable races.
    
    Returns multiplier 0.7-1.0
    """
    if fsi >= FSI_HIGH_COMPETITION_THRESHOLD:
        # Very competitive field - reduce confidence
        # Linear scale from 1.0 at threshold to 0.7 at FSI=1.0
        reduction = (fsi - FSI_HIGH_COMPETITION_THRESHOLD) / (1.0 - FSI_HIGH_COMPETITION_THRESHOLD)
        return max(0.7, 1.0 - (reduction * 0.3))
    return 1.0


def calculate_speed_factor(runner: Dict) -> float:
    """
    v3.5 NEW: Speed/Time Factor
    
    Compares runner's best time to estimate quality.
    Faster dogs get a boost in probability.
    
    Returns multiplier (0.95-1.10)
    """
    # Try to extract best time from runner data
    best_time = runner.get("best_time", None) or runner.get("time", None)
    
    if not best_time:
        return 1.0
    
    try:
        # Parse time (could be "29.85" or "29.85s")
        time_str = str(best_time).replace("s", "").strip()
        time_val = float(time_str)
        
        # Typical greyhound times: 20-45 seconds depending on distance
        # Lower time = faster = better
        # Use a simple bonus for fast times
        if time_val < 25:  # Very fast (short distance)
            return 1.08
        elif time_val < 30:  # Fast
            return 1.05
        elif time_val < 35:  # Average
            return 1.0
        elif time_val < 40:  # Slower
            return 0.97
        else:  # Slow
            return 0.95
    except (ValueError, TypeError):
        pass
    
    return 1.0


def get_track_box_prior(track: str) -> List[float]:
    """
    v3.5 NEW: Get track-specific box priors
    
    Returns track-specific box priors if available, otherwise default.
    """
    track_key = track.strip().lower() if track else ""
    
    # Check for partial matches (e.g., "Ladbrokes Gardens" -> "gardens")
    for known_track, priors in TRACK_BOX_PRIORS.items():
        if known_track in track_key or track_key in known_track:
            return priors
    
    # Check for common track name variations
    track_aliases = {
        "gardens": "meadows",
        "lakeside": "meadows",
        "townsville": "bendigo",  # Use balanced track default
    }
    
    for alias, target in track_aliases.items():
        if alias in track_key:
            return TRACK_BOX_PRIORS.get(target, BOX_PRIOR)
    
    return BOX_PRIOR


def update_trainer_wins(trainer: str, wins: int = 1) -> None:
    """Update trainer recent wins for momentum tracking."""
    if trainer:
        _trainer_recent_wins[trainer.strip().lower()] += wins


def reset_trainer_wins() -> None:
    """Reset trainer wins tracker (call at start of new day)."""
    _trainer_recent_wins.clear()


def build_features(rows: List[Dict], trainer_wins: Dict[str, int] = None) -> List[Dict]:
    """
    Build enhanced v3.5 features for all runners.
    
    Applies:
    - Track-specific box bias priors (Suggestion 1)
    - Enhanced Winning Streak boost with win weighting (Suggestion 3)
    - Closer Bonus for Box 7-8
    - Railer Bonus for Box 1-2 at short distances (Suggestion 5)
    - Reduced Trainer Momentum Factor (Suggestion 2)
    - Speed/Time Factor (Suggestion 4)
    - Adaptive FSI confidence (Suggestion 6)
    """
    # Group by (track,date,race) to compute field size and normalize priors
    groups = defaultdict(list)
    for r in rows:
        key = (r["track"], r["date"], r["race"])
        groups[key].append(r)

    feats: list[Dict] = []
    for key, runners in groups.items():
        track_name = key[0]  # Extract track name
        field_size = len(runners)
        
        # Suggestion 1: Use track-specific box priors
        track_priors = get_track_box_prior(track_name)
        priors = track_priors[:field_size]
        s = sum(priors) if priors else 1.0
        norm_priors = [p / s for p in priors]

        # Get race distance for closer/railer bonus calculation
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
        
        # Suggestion 6: Get FSI confidence multiplier
        fsi_confidence = get_fsi_confidence_multiplier(field_similarity)

        # Second pass: apply v3.5 enhancements and calculate raw adjusted priors
        adjusted_priors = []
        for rd in runner_data:
            r = rd["runner"]
            base_prior = rd["base_prior"]
            
            # Suggestion 3: Enhanced Winning Streak with win weighting
            streak_mult = calculate_winning_streak_boost(r)
            
            # v3.4 Closer Bonus for Box 7-8
            closer_bonus = calculate_closer_bonus(r, race_distance)
            
            # Suggestion 5: NEW Railer Bonus for Box 1-2 at short distances
            railer_bonus = calculate_railer_bonus(r, race_distance)
            
            # Suggestion 2: Reduced Trainer Momentum Factor
            trainer_boost = calculate_trainer_momentum(r, trainer_wins)
            
            # Suggestion 4: Speed/Time Factor
            speed_mult = calculate_speed_factor(r)
            
            # Calculate raw adjusted prior (before normalization)
            # Apply multiplicative factors first, then add bonuses
            raw_adjusted = (base_prior * streak_mult * speed_mult * fsi_confidence) + \
                          closer_bonus + railer_bonus + trainer_boost
            
            rd["streak_mult"] = streak_mult
            rd["closer_bonus"] = closer_bonus
            rd["railer_bonus"] = railer_bonus
            rd["trainer_boost"] = trainer_boost
            rd["speed_mult"] = speed_mult
            rd["fsi_confidence"] = fsi_confidence
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
            row["railer_bonus"] = rd["railer_bonus"]
            row["trainer_momentum"] = rd["trainer_boost"]
            row["speed_multiplier"] = rd["speed_mult"]
            row["fsi_confidence"] = rd["fsi_confidence"]
            row["field_similarity_index"] = field_similarity
            feats.append(row)
    
    return feats
