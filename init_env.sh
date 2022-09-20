#!/usr/bin/env bash

export PATH=/ocean/projects/hum160002p/shared/books/code/printprob-env/python/3.9.12/bin:${PATH}
module load anaconda3
conda create --name env python=3.9.12 netcdf4 -y
conda activate env
poetry install
