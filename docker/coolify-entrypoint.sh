#!/bin/sh
set -eu

python3 /MoneyPrinterTurbo/docker/coolify-entrypoint.py
exec "$@"
