#!/bin/bash
#SBATCH --job-name=gold
#SBATCH --output=/users/hguo55/scratch/github-lakehouse/logs/gold-%j.log
#SBATCH --error=/users/hguo55/scratch/github-lakehouse/logs/gold-%j.log
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --mem=8G

module load openjdk/17.0.11_9-7sqr
export PYSPARK_DRIVER_MEMORY=6g
cd /users/hguo55/projects/github-lakehouse
python3 gold/repo_daily.py
