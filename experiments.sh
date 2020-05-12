#!/bin/bash

LIB=$HOME/lib/diversity-maximization-experiments-assembly-0.1.0.jar
DATASETS_DIR=~/Datasets

function spark () {
  CLASS=$1
  EXECS=$2
  shift
  shift
  ARGS=$@
  echo "Running Spark with class $CLASS, library $LIB and arguments $ARGS on $EXECS executors"
  spark-submit \
       --num-executors $EXECS --executor-memory 8G --executor-cores 1 \
       --properties-file ~/Configurations/standalone-128.properties \
       --class $CLASS $LIB $ARGS
}

function streaming() {
  DATASET=$1
  FACTORS="1 2 4 8 16 32"
  TAUS="8 16 32 64 128 256"
  if [[ $DATASET =~ "glove" ]]
  then
    local SPECIAL_ARGS="--uniform glove"
    local TARGETS="100 25"
    local TARGETS="25"
  elif [[ $DATASET =~ "mxm" ]]
  then
    local SPECIAL_ARGS="--genres genres.rank88.txt"
    local TARGETS="88 22"
    local TARGETS="22"
  elif [[ $DATASET =~ "wiki" ]]
  then
    local SPECIAL_ARGS="--topics"
    local TARGETS="100 25"
    local TARGETS="25"
  fi
  for TARGET in $TARGETS
  do 
    for TAU in $TAUS
    do
      spark it.unipd.dei.diversity.MainMatroid 9 \
        --algorithm streaming \
        --input $DATASET \
        --target $TARGET \
        --sparsify \
        --tau $TAU \
        $SPECIAL_ARGS
    done
  done
}

function mapreduce() {
  DATASET=$1
  FACTORS="1 2 4 8"
  if [[ $DATASET =~ "glove" ]]
  then
    local SPECIAL_ARGS="--uniform glove"
    local TARGETS="25 100"
  elif [[ $DATASET =~ "mxm" ]]
  then
    local SPECIAL_ARGS="--genres genres.rank88.txt"
    local TARGETS="22 88"
  elif [[ $DATASET =~ "wiki" ]]
  then
    local SPECIAL_ARGS="--topics"
    local TARGETS="100 25"
  fi
  for TARGET in $TARGETS
  do 
    for PARALLELISM in 1 2 4 8 16
    do
      local TAU=64
      local TAU_P=$(( 64 / $PARALLELISM ))
      spark it.unipd.dei.diversity.MainMatroid $PARALLELISM \
        --algorithm mapreduce \
        --parallelism $PARALLELISM \
        --input $DATASET \
        --target $TARGET \
        --sparsify \
        --tau $TAU \
        --tau-p $TAU_P \
        $SPECIAL_ARGS
    done
  done
}

function sequential() {
  DATASET=$1
  TAUS="8 16 32 128 256"
  if [[ $DATASET =~ "glove" ]]
  then
    local SPECIAL_ARGS="--uniform glove"
    local TARGETS="25 100"
  elif [[ $DATASET =~ "mxm" ]]
  then
    local SPECIAL_ARGS="--genres genres.rank88.txt"
    local TARGETS="22 88"
  elif [[ $DATASET =~ "wiki" ]]
  then
    local SPECIAL_ARGS="--topics"
    local TARGETS="100 25"
  fi
  for TARGET in $TARGETS
  do 
    for TAU in $TAUS
    do
      spark it.unipd.dei.diversity.MainMatroid 9 \
        --algorithm sequential-coreset \
        --input $DATASET \
        --target $TARGET \
        --sparsify \
        --tau $TAU \
        $SPECIAL_ARGS
    done
  done
}


streaming mxm.pq
streaming glove.twitter.27B.25d.pq
streaming wikipedia-25.json.bz2

mapreduce mxm.pq
mapreduce glove.twitter.27B.25d.pq
mapreduce wikipedia-25.json.bz2

sequential mxm-5000.pq
sequential glove-5000.pq
sequential wikipedia-5000.json

