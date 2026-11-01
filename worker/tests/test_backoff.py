from orch_worker.backoff import Backoff


def test_ceiling_doubles_up_to_the_cap():
    b = Backoff(base=0.5, cap=30.0, rng=lambda: 1.0)  # rng=1 exposes the ceiling
    assert [b.next() for _ in range(9)] == [0.5, 1, 2, 4, 8, 16, 30, 30, 30]


def test_delays_stay_within_zero_and_the_cap():
    b = Backoff(base=0.5, cap=30.0)
    delays = [b.next() for _ in range(200)]  # also exercises a large exponent
    assert all(0 <= d <= 30.0 for d in delays)


def test_reset_starts_over():
    b = Backoff(base=0.5, cap=30.0, rng=lambda: 1.0)
    for _ in range(5):
        b.next()
    b.reset()
    assert b.next() == 0.5
