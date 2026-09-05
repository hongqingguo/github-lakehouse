#!/bin/bash
#SBATCH --job-name=gh-download
#SBATCH --output=/users/hguo55/scratch/github-lakehouse/logs/download-%j.log
#SBATCH --error=/users/hguo55/scratch/github-lakehouse/logs/download-%j.log
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=2G

cd /users/hguo55/projects/github-lakehouse
python3 scripts/download_gharchive.py --start 2026-07-15 --end 2026-08-11
