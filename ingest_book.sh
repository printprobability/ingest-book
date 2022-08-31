#!/usr/bin/env bash

# add book
source ~/.bashrc; module load anaconda3; conda activate /jet/home/nikolaiv/miniconda3/envs/py39;
poetry update
poetry run chewfiles "$@"
