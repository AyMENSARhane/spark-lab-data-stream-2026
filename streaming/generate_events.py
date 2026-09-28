"""Publish synthetic movie plays as complete JSON-lines files, using stdlib only."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import random
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "streaming" / "runtime" / "events"


class EventGenerator:
    """Seed controls users/movies; timestamps follow the supplied clock."""

    def __init__(self, seed=42):
        with (ROOT / "data" / "movies.csv").open(encoding="utf-8", newline="") as file:
            self.movies = list(csv.DictReader(file))[:20]
        self.ids = [int(movie["movieId"]) for movie in self.movies]
        self.weights = [1 / (index + 1) for index in range(len(self.ids))]
        self.rng = random.Random(seed)
        self.batch_index = 0

    def batch(self, when, size=40):
        # 20 seconds baseline, 30 seconds spike, 10 seconds recovery at defaults.
        phase = self.batch_index % 30
        cycle = self.batch_index // 30
        trending_index = 10 + cycle % 10
        trending = self.ids[trending_index] if 10 <= phase < 25 else None
        events = []
        for _ in range(size):
            movie = (
                trending if trending is not None and self.rng.random() < 0.80
                else self.rng.choices(self.ids, weights=self.weights, k=1)[0]
            )
            events.append({
                "timestamp": when.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
                "userId": self.rng.randint(1, 610),
                "movieId": movie,
                "eventType": "play",
            })
        self.batch_index += 1
        title = self.movies[trending_index]["title"] if trending is not None else None
        return events, title


def publish_batch(directory, events):
    """Rename on the same filesystem; Spark ignores underscore-prefixed files."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    name = uuid.uuid4().hex
    temporary = directory / f"_pending-{name}.json"
    destination = directory / f"events-{name}.json"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as file:
            for event in events:
                file.write(json.dumps(event) + "\n")
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def seed_demo(directory):
    """Small recent replay so Run All works even before the live producer starts."""
    generator = EventGenerator()
    now = datetime.now(timezone.utc)
    for batch_index in range(20):
        events, _ = generator.batch(now - timedelta(seconds=40 - batch_index * 2))
        publish_batch(directory, events)


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def nonnegative_float(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite nonnegative number")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--interval", type=nonnegative_float, default=2.0, help="seconds between files (default: 2)")
    parser.add_argument("--events-per-batch", type=positive_int, default=40)
    parser.add_argument("--batches", type=positive_int, help="stop after this many files; default: run until Ctrl+C")
    args = parser.parse_args()
    if args.interval == 0 and args.batches is None:
        parser.error("--interval 0 requires --batches to avoid an unbounded fast producer")
    generator = EventGenerator(args.seed)
    print(f"Writing plays to {args.output_dir.resolve()}", flush=True)
    print("Ctrl+C stops the producer. Existing files are kept for replay.", flush=True)
    count = 0
    try:
        while args.batches is None or count < args.batches:
            events, title = generator.batch(datetime.now(timezone.utc), args.events_per_batch)
            path = publish_batch(args.output_dir, events)
            count += 1
            phase = f"TREND: {title}" if title else "baseline popularity"
            print(f"{count:04d} | {len(events)} plays | {phase} | {path.name}", flush=True)
            if args.batches is None or count < args.batches:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nProducer stopped cleanly.")
    except OSError as exc:
        parser.exit(1, f"Could not publish events: {exc}\nCheck that the output directory is writable.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
