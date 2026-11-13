#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
if [[ -x .venv/bin/python ]]; then
  python_cmd=.venv/bin/python
elif [[ -x .venv/Scripts/python.exe ]]; then
  python_cmd=.venv/Scripts/python.exe
else
  python_cmd="${PYTHON:-python3}"
fi
exec "$python_cmd" -m payments teardown "$@"
