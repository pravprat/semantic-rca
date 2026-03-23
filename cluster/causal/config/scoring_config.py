# cluster/causal/config/scoring_config.py

EDGE_SCORING = {
    "base_temporal_weight": 0.7,
    "semantic_weight_per_match": 0.1,
    "max_score": 1.0,
}

CANDIDATE_SCORING = {
    "trigger_weight": 0.5,
    "temporal_weight": 0.3,
    "graph_weight": 0.2,
}