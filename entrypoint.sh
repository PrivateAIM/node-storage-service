#!/bin/sh
set -e

python project/migrations/scripts/migrate.py
python -m uvicorn project.main:app "$@"
