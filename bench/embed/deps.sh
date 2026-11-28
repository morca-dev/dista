#!/bin/sh
# Checks out parquet.mojo and the pure-Mojo tins it compiles with under
# build/embed-deps/, at the revisions the data job in
# .github/workflows/ci.yml checks out, so there is one list of them. The
# embed environment's EMBED_INC points there (pixi.toml). A checkout already
# at its revision is left alone.
set -eu
deps=build/embed-deps
mkdir -p "$deps"
for name in parquet thrift hashes snappy deflate threads; do
    rev=$(sed -n "s|.*repository: magmalake/$name.mojo, ref: \([0-9a-f]*\),.*|\1|p" \
        .github/workflows/ci.yml | head -n 1)
    if [ -z "$rev" ]; then
        echo "deps.sh: no revision of $name.mojo in .github/workflows/ci.yml" >&2
        exit 1
    fi
    dir="$deps/$name.mojo"
    if [ -d "$dir/.git" ] && [ "$(git -C "$dir" rev-parse HEAD)" = "$rev" ]; then
        continue
    fi
    rm -rf "$dir"
    git init -q "$dir"
    git -C "$dir" fetch -q --depth 1 "https://github.com/magmalake/$name.mojo" "$rev"
    git -C "$dir" -c advice.detachedHead=false checkout -q FETCH_HEAD
    echo "deps.sh: $name.mojo at $rev"
done
