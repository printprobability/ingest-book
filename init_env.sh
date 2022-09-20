#!/usr/bin/env bash

export PATH=/ocean/projects/hum160002p/shared/books/code/printprob-env/python/3.9.12/bin:${PATH}
module load anaconda3
CONDA_ENV_CREATED=$(conda info -e  | grep ".conda/envs/env")
if [ $retVal != 0 ]; then
  conda create --name env python=3.9.12 netcdf4 -y
fi
conda activate env
poetry install
