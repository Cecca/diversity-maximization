#!/bin/bash

# This script downloads, converts, creates datasets used in the experiments

LIB=experiments/target/scala-2.11/diversity-maximization-experiments-assembly-0.1.0.jar
# DATASETS_DIR=hdfs://hdfs-master/path/to/datasets/directory
DATASETS_DIR=~/Datasets/diversity

SPARK_COMMAND=spark-submit --class it.unipd.dei.diversity.BagOfWordsDataset $LIB
SPARK_COMMAND=java -Dspark.master='local[*]' -Dspark.driver.host="localhost" -cp $LIB

function Glove () {
  URL="http://nlp.stanford.edu/data/glove.twitter.27B.zip"
  COMPRESSED_DATA=glove.twitter.27B.zip

  pushd $HOME/Datasets
  test -f COMPRESSED_DATA || wget $URL
  popd
}

function MusixMatch () {
  pushd $HOME/Datasets
  DATA_FILE="mxm_data_file.txt"
  TRAIN_FILE="mxm_dataset_train.txt"
  TEST_FILE="mxm_dataset_test.txt"
  TRAIN_FILE_URL="http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset_train.txt.zip"
  TEST_FILE_URL="http://millionsongdataset.com/sites/default/files/AdditionalFiles/mxm_dataset_test.txt.zip"
  test -f $TRAIN_FILE || wget $TRAIN_FILE_URL 
  test -f $TRAIN_FILE || unzip $TRAIN_FILE
  test -f $TEST_FILE || wget $TEST_FILE_URL
  test -f $TEST_FILE || unzip $TEST_FILE

  test -f $DATA_FILE || cat $TEST_FILE $TRAIN_FILE > $DATA_FILE

  spark-submit \
    --class it.unipd.dei.diversity.BagOfWordsDataset \
    $LIB \
    --format mxm --input mxm.txt \
    --output $DATASETS_DIR/mxm-bigger10.bow --transform "bigger(10)"

  popd
}

function Songs () {
  # You should log in to kaggle and download the raw data from https://www.kaggle.com/gyani95/380000-lyrics-from-metrolyrics
  pushd $HOME/Datasets
  COMPRESSED_DATA="songs.zip"
  DATA="lyrics.csv"

  test -f $COMPRESSED_DATA || exit 1
  test -f $DATA || unzip $COMPRESSED_DATA

  popd
}

# MusixMatch
Songs
# Glove

