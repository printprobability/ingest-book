#!/usr/bin/env bash

export PATH=/ocean/projects/hum160002p/shared/books/code/printprob-env/python/3.9.12/bin:${PATH}
echo "Loading anaconda3 module..."
module load anaconda3
OUT=$(conda info --envs  | grep ".conda/envs/env")
retVal=$?
if [ $retVal != 0 ]; then
  echo "Conda env not found, creating one."
  conda create --name env python=3.9.12 netcdf4 -y
fi
echo "Activating Conda Env.."
conda activate env
echo "Activated."
echo "Installing Poetry Dependencies..."
poetry install
source /ocean/projects/hum160002p/shared/books/code/printprob-env/.env
echo "Completed env setup."
