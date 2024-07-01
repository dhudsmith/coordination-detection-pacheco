#!/bin/bash
#SBATCH --job-name=pacheco
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=16
#SBATCH --mem=64G
#SBATCH --time=2:00:00

# Activate the conda environment
source activate pacheco

# Change to the directory where your Snakemake file is located
cd ~/Code/coordinated-detection-pacheco

# Run Snakemake
snakemake --cores all