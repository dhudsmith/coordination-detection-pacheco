#!/bin/bash

# create and activate the env
conda create -y -n pacheco python=3.11
source activate pacheco

# install mamba
conda install -y -c conda-forge mamba

# install snakemake
mamba install -y -c conda-forge bioconda::snakemake

# install other packages via mamba
mamba install -y -c conda-forge --file requirements_conda.txt

# install other packages via pip
pip install --no-input -r requirements_pip.txt
