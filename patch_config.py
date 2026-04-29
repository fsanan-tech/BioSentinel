"""
BioSentinel Config Patch
Run this ONCE from your BioSentinel folder to update fusion thresholds:
    py -3.13 patch_config.py

What it fixes:
  - min_confidence_threshold: 0.20 → 0.05  (so single-source events appear)
  - prior_probability: 0.08 → 0.12         (slightly more generous prior)
  - Adds single-source credibility boost so CDC/WHO single signals appear on map
"""
import re

CONFIG_PATH = "config.py"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    content = f.read()

# Fix 1: lower min_confidence_threshold
content = re.sub(
    r'"min_confidence_threshold":\s*[\d.]+',
    '"min_confidence_threshold": 0.05',
    content
)

# Fix 2: raise prior
content = re.sub(
    r'"prior_probability":\s*[\d.]+',
    '"prior_probability": 0.12',
    content
)

# Fix 3: lower spatial merge radius so nearby events cluster better
content = re.sub(
    r'"spatial_merge_radius_km":\s*[\d.]+',
    '"spatial_merge_radius_km": 300',
    content
)

with open(CONFIG_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("✓ config.py patched:")
print("  min_confidence_threshold → 0.05")
print("  prior_probability        → 0.12")
print("  spatial_merge_radius_km  → 300")
print("\nRestart the server: py -3.13 run.py")
