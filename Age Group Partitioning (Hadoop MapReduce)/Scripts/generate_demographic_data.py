#!/usr/bin/env python3
"""
generate_demographic_data.py
────────────────────────────
Generates a large synthetic demographic CSV dataset suitable for the
AgeGroup MapReduce job.  The script is optimised for producing ≥ 1 GB of
data quickly with realistic distributions.

Usage
─────
    # Default: write to ./demographic_data.csv (≈ 1 GB)
    python3 generate_demographic_data.py

    # Custom size and output path
    python3 generate_demographic_data.py --size-gb 2 --output /tmp/demo.csv

    # Quick sample (8 rows, mimics the task spec exactly)
    python3 generate_demographic_data.py --sample

Output format (CSV, no header required by Mapper but written for clarity)
────────────────────────────────────────────────────────────────────────
    person_id, age, income, employment_status, education_level
    P000000001,25,3000,Employed,Bachelor
    ...

Design decisions
────────────────
* Uses buffered writes (8 MB chunks) to keep memory usage flat regardless
  of output size — safe for datasets much larger than RAM.
* person_id is a zero-padded integer prefixed with "P" for readability.
* Ages follow a normal distribution centred at 38 (σ=14), clipped to
  18–80, yielding realistic proportions across all five age groups.
* Incomes are correlated with age group and education level, matching
  real-world patterns, with ±15 % random noise.
* Approximately 3 % of records contain deliberate defects (missing fields,
  bad numbers) to exercise the Mapper's error-handling paths.

Requirements
────────────
    Python ≥ 3.8   (no third-party packages needed)
"""

from __future__ import annotations

import argparse
import io
import math
import os
import random
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ────────────────────────────────────────────────────────────────────────────
# Configuration constants
# ────────────────────────────────────────────────────────────────────────────

HEADER: str = "person_id,age,income,employment_status,education_level\n"

EMPLOYMENT_STATUSES: List[str] = [
    "Employed", "Employed", "Employed", "Employed",   # 4× weight
    "Self-Employed",
    "Unemployed",
    "Student",
    "Retired",
]

EDUCATION_LEVELS: List[str] = [
    "High School",
    "Associate",
    "Bachelor", "Bachelor", "Bachelor",               # 3× weight
    "Master", "Master",                               # 2× weight
    "PhD",
]

# Base income multipliers by education
EDUCATION_INCOME_MULTIPLIER: Dict[str, float] = {
    "High School": 0.65,
    "Associate":   0.80,
    "Bachelor":    1.00,
    "Master":      1.30,
    "PhD":         1.60,
}

# Base income by age group ($)
AGE_GROUP_BASE_INCOME: Dict[str, float] = {
    "18-24": 22_000,
    "25-34": 38_000,
    "35-44": 52_000,
    "45-54": 58_000,
    "55+":   48_000,  # some retired, reduced slightly
}

NOISE_FACTOR: float = 0.15   # ±15 % random noise on income
DEFECT_RATE:  float = 0.03   # 3 % of records are malformed

BUFFER_SIZE:  int   = 8 * 1024 * 1024   # 8 MB write buffer
REPORT_EVERY: int   = 500_000           # print progress every N records


# ────────────────────────────────────────────────────────────────────────────
# Helper functions
# ────────────────────────────────────────────────────────────────────────────

def age_group(age: int) -> str:
    """Return the age-group bucket label for a given age."""
    if age <= 24: return "18-24"
    if age <= 34: return "25-34"
    if age <= 44: return "35-44"
    if age <= 54: return "45-54"
    return "55+"


def generate_age() -> int:
    """
    Sample age from a normal distribution (μ=38, σ=14) clipped to [18, 80].
    This gives realistic proportions: largest group 25-34 / 35-44,
    smaller tails at 18-24 and 55+.
    """
    while True:
        a = int(random.gauss(38, 14))
        if 18 <= a <= 80:
            return a


def generate_income(age: int, education: str) -> int:
    """
    Compute a realistic income with noise, rounded to nearest 100.
    Income = base_for_age_group × education_multiplier × (1 ± noise)
    """
    group  = age_group(age)
    base   = AGE_GROUP_BASE_INCOME[group]
    mult   = EDUCATION_INCOME_MULTIPLIER.get(education, 1.0)
    noise  = random.uniform(1 - NOISE_FACTOR, 1 + NOISE_FACTOR)
    raw    = base * mult * noise
    return max(0, int(round(raw / 100) * 100))


def generate_employment(age: int) -> str:
    """Adjust employment status based on age for realism."""
    if age <= 24:
        pool = ["Student", "Student", "Employed", "Unemployed"]
    elif age >= 62:
        pool = ["Retired", "Retired", "Employed", "Self-Employed"]
    else:
        pool = EMPLOYMENT_STATUSES
    return random.choice(pool)


def make_record(person_id: int) -> str:
    """Generate one clean CSV record."""
    pid        = f"P{person_id:09d}"
    age        = generate_age()
    education  = random.choice(EDUCATION_LEVELS)
    employment = generate_employment(age)
    income     = generate_income(age, education)
    return f"{pid},{age},{income},{employment},{education}\n"


def make_defective_record(person_id: int) -> str:
    """
    Generate a malformed record to exercise error-handling in the Mapper.
    Defect types chosen at random:
      0 – missing fields
      1 – non-numeric age
      2 – non-numeric income
      3 – negative age
      4 – empty line
    """
    pid   = f"P{person_id:09d}"
    dtype = random.randint(0, 4)
    if dtype == 0:
        return f"{pid},25\n"                      # missing fields
    elif dtype == 1:
        return f"{pid},twentyfive,3000,Employed,Bachelor\n"
    elif dtype == 2:
        return f"{pid},25,N/A,Employed,Bachelor\n"
    elif dtype == 3:
        return f"{pid},-5,3000,Employed,Bachelor\n"
    else:
        return "\n"                                # blank line


# ────────────────────────────────────────────────────────────────────────────
# Sample data (mirrors the 8 rows from the task specification)
# ────────────────────────────────────────────────────────────────────────────

SAMPLE_ROWS: str = """\
person_id,age,income,employment_status,education_level
P001,25,3000,Employed,Bachelor
P002,35,5000,Employed,Master
P003,45,6000,Employed,Bachelor
P004,22,2500,Student,High School
P005,55,7000,Employed,PhD
P006,28,4000,Employed,Bachelor
P007,38,5500,Self-Employed,Master
P008,62,6500,Retired,Bachelor
"""


# ────────────────────────────────────────────────────────────────────────────
# Main generation logic
# ────────────────────────────────────────────────────────────────────────────

def generate_dataset(output_path: str, target_bytes: int) -> None:
    """
    Write synthetic demographic records to *output_path* until the file
    reaches *target_bytes* (measured periodically, not per-record, so the
    final file may be slightly larger than the target).

    Args:
        output_path:  Filesystem path for the output CSV file.
        target_bytes: Approximate target file size in bytes.
    """
    print(f"[generator] Target size : {target_bytes / (1024**3):.2f} GB")
    print(f"[generator] Output file : {output_path}")
    print(f"[generator] Defect rate : {DEFECT_RATE*100:.1f}%")
    print(f"[generator] Buffer size : {BUFFER_SIZE // (1024*1024)} MB")

    start_time   = time.time()
    person_id    = 1
    bytes_written = 0
    records_written = 0

    with open(output_path, "w", buffering=BUFFER_SIZE, encoding="utf-8") as f:
        # Write CSV header
        f.write(HEADER)
        bytes_written += len(HEADER.encode("utf-8"))

        buf = io.StringIO()
        buf_bytes = 0

        while bytes_written < target_bytes:
            # ── Generate record (clean or defective) ─────────────────
            if random.random() < DEFECT_RATE:
                line = make_defective_record(person_id)
            else:
                line = make_record(person_id)

            buf.write(line)
            line_bytes = len(line.encode("utf-8"))
            buf_bytes    += line_bytes
            bytes_written += line_bytes
            person_id    += 1
            records_written += 1

            # Flush string buffer to file
            if buf_bytes >= BUFFER_SIZE:
                f.write(buf.getvalue())
                buf.truncate(0)
                buf.seek(0)
                buf_bytes = 0

            # ── Progress report ───────────────────────────────────────
            if records_written % REPORT_EVERY == 0:
                elapsed = time.time() - start_time
                pct     = bytes_written / target_bytes * 100
                mb      = bytes_written / (1024 ** 2)
                speed   = mb / elapsed if elapsed > 0 else 0
                print(f"  {pct:5.1f}%  {mb:9.1f} MB  "
                      f"{records_written:,} records  "
                      f"{speed:.1f} MB/s", flush=True)

        # Flush remainder
        if buf_bytes > 0:
            f.write(buf.getvalue())

    elapsed   = time.time() - start_time
    final_mb  = os.path.getsize(output_path) / (1024 ** 2)
    final_gb  = final_mb / 1024
    speed     = final_mb / elapsed if elapsed > 0 else 0

    print(f"\n[generator] Done!")
    print(f"  Records written : {records_written:,}")
    print(f"  Final file size : {final_mb:.1f} MB  ({final_gb:.3f} GB)")
    print(f"  Elapsed         : {elapsed:.1f} s")
    print(f"  Avg throughput  : {speed:.1f} MB/s")


# ────────────────────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────────────────────

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large synthetic demographic CSV for the AgeGroup MapReduce job.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--size-gb", type=float, default=1.0,
        metavar="GB",
        help="Target output file size in gigabytes (e.g. 1.5 for 1.5 GB).",
    )
    parser.add_argument(
        "--output", "-o", type=str, default="demographic_data.csv",
        metavar="PATH",
        help="Output file path.",
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Write only the 8-row sample from the task spec and exit.",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        metavar="INT",
        help="Random seed for reproducibility (omit for random behaviour).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)
        print(f"[generator] Random seed: {args.seed}")

    if args.sample:
        sample_path = args.output if args.output != "demographic_data.csv" \
                      else "sample_input.csv"
        print(f"[generator] Writing sample data → {sample_path}")
        with open(sample_path, "w", encoding="utf-8") as f:
            f.write(SAMPLE_ROWS)
        print("[generator] Done.")
        return 0

    if args.size_gb <= 0:
        print("[generator] ERROR: --size-gb must be > 0", file=sys.stderr)
        return 1

    target_bytes = int(args.size_gb * 1024 ** 3)
    generate_dataset(args.output, target_bytes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
    
# cd "F:\faculty\Level 3 S_2\Introduction to Big Data\Project\HadoopMapReduce\Age Group Partitioning (Hadoop MapReduce)"
# python Scripts/generate_demographic_data.py --size-gb 1 --output "Data/demographic_data.csv"