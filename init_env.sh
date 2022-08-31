#!/usr/bin/env zsh

source ~/.bashrc
module load anaconda3
eval "$(conda shell.bash hook)"
conda activate /ocean/projects/hum160002p/gsell/.conda/envs/my_env;
