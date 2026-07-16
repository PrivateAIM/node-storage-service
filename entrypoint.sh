#!/bin/sh
set -e

python project/migrations/scripts/migrate.py
exec python -m uvicorn project.main:app "$@"
