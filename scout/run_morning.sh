#!/usr/bin/env bash
# Run Morning Runner; writes opportunities.json + today.json (inside morning_runner.py)
set -e
cd "$(dirname "$0")"
exec python3 morning_runner.py
