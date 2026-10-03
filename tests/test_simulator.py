import random
import statistics

from edge.mqtt_simulator import METRICS, make_reading


def test_simulated_values_stay_physical_and_near_baseline():
    rng = random.Random(1)
    state: dict[str, float] = {}
    for metric, (baseline, _noise) in METRICS.items():
        values = [make_reading("dev-001", metric, state, rng).value for _ in range(20_000)]
        assert min(values) >= 0.0, metric
        # mean-reverting: long-run median stays within 10% of baseline (spikes excluded)
        assert abs(statistics.median(values) - baseline) / baseline < 0.10, metric
