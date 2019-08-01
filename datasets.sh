#!/bin/bash

# This script downloads, converts, creates datasets used in the experiments

# LIB=experiments/target/scala-2.11/diversity-maximization-experiments-assembly-0.1.0.jar
LIB=$HOME/lib/diversity-maximization-experiments-assembly-0.1.0.jar
# DATASETS_DIR=hdfs://hdfs-master/path/to/datasets/directory
DATASETS_DIR=~/Datasets/diversity

function spark () {
  CLASS=$1
  shift
  ARGS=$@
  echo "Running Spark with class $CLASS, library $LIB and arguments $ARGS"
  spark-submit --master local --class $CLASS $LIB $ARGS
}

function Glove () {
  URL="http://nlp.stanford.edu/data/glove.twitter.27B.zip"
  COMPRESSED_DATA=glove.twitter.27B.zip
  PQ_25=glove.twitter.27B.25d.pq
  PQ_50=glove.twitter.27B.50d.pq

  pushd $HOME/Datasets
  test -f $COMPRESSED_DATA || wget $URL
  test -f glove.twitter.27B.25d.txt || unzip $COMPRESSED_DATA

  test -d $PQ_25 || spark \
    it.unipd.dei.diversity.matroid.GloVePoint \
    --input glove.twitter.27B.25d.txt \
    --output $PQ_25

  test -d $PQ_50 || spark \
    it.unipd.dei.diversity.matroid.GloVePoint \
    --input glove.twitter.27B.50d.txt \
    --output $PQ_50

  popd
}

function MusixMatch () {
  pushd $HOME/Datasets
  DATA_FILE="mxm_data.txt"
  PQ_FILE="mxm.pq"
  TRAIN_FILE="mxm_dataset_train.txt"
  TEST_FILE="mxm_dataset_test.txt"
  TRAIN_FILE_URL="http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset_train.txt.zip"
  TEST_FILE_URL="http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset_test.txt.zip"
  GENRES_URL="http://www.tagtraum.com/genres/msd_tagtraum_cd2.cls.zip"
  GENRES_FILE="msd_tagtraum_cd2.cls"
  test -f $TRAIN_FILE || wget $TRAIN_FILE_URL 
  test -f $TRAIN_FILE || unzip $TRAIN_FILE
  test -f $TEST_FILE || wget $TEST_FILE_URL
  test -f $TEST_FILE || unzip $TEST_FILE
  test -f $(basename $GENRES_URL) || wget $GENRES_URL
  test -f $GENRES_FILE || unzip $(basename $GENRES_URL)

  test -f $DATA_FILE || cat $TEST_FILE $TRAIN_FILE > $DATA_FILE

  test -d $PQ_FILE || spark it.unipd.dei.diversity.matroids.Song \
    --input $DATA_FILE \
    --output $PQ_FILE \
    --genres $GENRES_FILE

  popd
}

function Songs () {
  # You should log in to kaggle and download the raw data from https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics
  pushd $HOME/Datasets
  COMPRESSED_DATA="songs.zip"
  DATA="lyrics.csv"

  test -f $COMPRESSED_DATA || exit 1
  test -f $DATA || unzip $COMPRESSED_DATA

  popd
}

MusixMatch
# Glove

