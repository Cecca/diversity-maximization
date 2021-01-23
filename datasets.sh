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

function Wiki {
  # Install a newer python
  if [[ ! -f ~/.python-3.6/bin/python3 ]]
  then
    mkdir python-3.6
    pushd python-3.6
    wget https://www.python.org/ftp/python/3.6.9/Python-3.6.9.tgz
    tar -zxvf Python-3.6.9.tgz
    pushd Python-3.6.9
    mkdir ~/.python-3.6
    ./configure --prefix=$HOME/.python-3.6
    make
    make install
    popd
    popd
  fi
  # Create the virtualenv
  ~/.python-3.6/bin/python3 -m venv python-env
  source python-env/bin/activate
  #pip install gensim
  pip install -r requirements.txt
  SCRIPT=$(pwd)/wiki_preprocess.py

  Glove # Gets the vectors for the remapping
  pushd $DATASETS_DIR
  URL="https://dumps.wikimedia.org/enwiki/20190720/enwiki-20190720-pages-articles-multistream.xml.bz2"
  EXTRACTOR_URL="https://raw.githubusercontent.com/attardi/wikiextractor/3162bb6c3c9ebd2d15be507aa11d6fa818a454ac/WikiExtractor.py"
  WIKI_DIR="wiki-raw"
  WIKI_OUTPUT="wikipedia-25.json.bz2"

  test -f $(basename $EXTRACTOR_URL) || wget $EXTRACTOR_URL
  test -f $(basename $URL) || wget $URL
  test -d $WIKI_DIR || bzcat $(basename $URL) | python ./WikiExtractor.py \
    --json --no_templates \
    -c -o $WIKI_DIR -
  test -f $WIKI_OUTPUT || python $SCRIPT $WIKI_DIR glove.twitter.27B.100d.txt $WIKI_OUTPUT
  hdfs dfs -put $WIKI_OUTPUT
  touch $WIKI_OUTPUT.metadata
  hdfs dfs -put $WIKI_OUTPUT.metadata
}

MusixMatch
# Glove
Wiki

