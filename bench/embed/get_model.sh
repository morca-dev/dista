#!/bin/sh
# Downloads BAAI/bge-small-en-v1.5, the census's model, at one pinned
# revision into EMBED_MODEL (default build/embed/models/bge-small-en-v1.5):
# the files encoder.mojo's Encoder(dir) reads, as its tools/get_models.sh
# fetches them. Files already present are skipped.
set -eu
model=BAAI/bge-small-en-v1.5
rev=5c38ec7c405ec4b44b94cc5a9bb96e735b38267a
dir=${EMBED_MODEL:-build/embed/models/bge-small-en-v1.5}
for f in config.json vocab.txt 1_Pooling/config.json model.safetensors; do
    [ -s "$dir/$f" ] && continue
    mkdir -p "$(dirname "$dir/$f")"
    curl -fsSL --retry 3 -o "$dir/$f.part" \
        "https://huggingface.co/$model/resolve/$rev/$f"
    mv "$dir/$f.part" "$dir/$f"
    echo "get_model.sh: $dir/$f"
done
echo "get_model.sh: $model at $rev in $dir"
