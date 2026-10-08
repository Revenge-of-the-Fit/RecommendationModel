#!/usr/bin/env bash
# Clone the four model repos read-only into external/ (idempotent).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p external
clone() { [ -d "external/$1" ] || git clone --depth 1 "$2" "external/$1"; }
# Helixan's model is used from this repo's src/ when present (see model_comparison/repos.py)
[ -f src/recommender.py ] || clone helixan https://github.com/Helixan/RecommendationModel
clone urbonas  https://github.com/d-urbonas/CMU-MLinProd
clone muhammad https://github.com/MuhammadDF/S3D17645_M0
clone rec-zilla https://github.com/MajorTomLanded/rec-zilla
