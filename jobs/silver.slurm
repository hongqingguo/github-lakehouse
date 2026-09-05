#!/bin/bash
#SBATCH --job-name=silver
#SBATCH --output=/users/hguo55/scratch/github-lakehouse/logs/silver-%j.log
#SBATCH --error=/users/hguo55/scratch/github-lakehouse/logs/silver-%j.log
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --mem=32G

module load openjdk/17.0.11_9-7sqr
export PYSPARK_DRIVER_MEMORY=24g
cd /users/hguo55/projects/github-lakehouse
python3 silver/transform.py
