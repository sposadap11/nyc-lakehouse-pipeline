# Git Commit Guide

Run these commands in order within the project root:

# 1. Initialize and base structure

git init
git add src/common/config.py
git commit -m "chore: init project structure"

# 2. Bronze Layer

git add src/etl/bronze.py notebooks/01_bronze_ingest.ipynb
git commit -m "feat: bronze ingestion delta"

# 3. Silver Layer

git add src/etl/silver.py notebooks/02_silver_transform.ipynb
git commit -m "feat: silver transform + dq + merge idempotent"

# 4. Gold Layer

git add src/etl/gold.py notebooks/03_gold_kpis_daily.ipynb
git commit -m "feat: gold daily kpis"

# 5. Documentation and scripts

git add README.md
git commit -m "docs: add README + defense script"

# 6. Push to GitHub (Replace <URL>)

# git remote add origin <your-repo-url>

# git branch -M main

# git push -u origin main
