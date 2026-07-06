#!/usr/bin/env bash
# Create the board.events topic once Redpanda is up.
set -euo pipefail

docker exec kanban-redpanda rpk topic create board.events \
  --partitions 1 --replicas 1 || true

docker exec kanban-redpanda rpk topic list
