#!/usr/bin/env bash
# Runs Spotless formatting and style checks across all Maven modules.
if command -v mvn.cmd >/dev/null 2>&1; then
    mvn.cmd spotless:check "$@"
else
    mvn spotless:check "$@"
fi
