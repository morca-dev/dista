#!/bin/sh
# Builds test binaries several at once, with the arguments their pixi tasks
# build them with, so each task's own build is a copy (scripts/mojo-build.sh).
# One `mojo build` keeps under two cores busy; this runs one per two cores.
#
# Usage: scripts/build-tests.sh [source.mojo...]
# With no sources: the unit tests' one binary (scripts/test.sh --build),
# tests/test_libc_abi.mojo and tests/test_head_loop.mojo, which are built
# on their own, and cli/main.mojo,
# which three of the shell tests build.
set -eu
if [ "$#" -eq 0 ]; then
    set -- tests/test_libc_abi.mojo tests/test_head_loop.mojo cli/main.mojo
    sh scripts/test.sh --build &
    units=$!
fi
cores=$(getconf _NPROCESSORS_ONLN 2> /dev/null || echo 2)
jobs=$((cores / 2 + 1))
mkdir -p build
# shellcheck disable=SC2016
printf '%s\n' "$@" | xargs -n 1 -P "$jobs" sh -c '
    case "$1" in
    cli/main.mojo) exec scripts/mojo-build.sh -I src ${DEP_INC:--I src} "$1" -o build/dista ;;
    *) exec scripts/mojo-build.sh -I src $DEP_INC "$1" -o "build/$(basename "$1" .mojo | tr _ -)" ;;
    esac' sh
[ -z "${units:-}" ] || wait "$units"
