========================
 Diversity maximization
========================

.. image:: https://travis-ci.org/Cecca/diversity-maximization.svg?branch=master
   :target: https://travis-ci.org/Cecca/diversity-maximization

This repository contains code implementing the algorithms presented in
[CeccarelloPPU16]_. From the abstract of the paper

 Given a dataset of points in a metric space and an integer k, a
 diversity maximization problem requires determining a subset of k
 points maximizing some diversity objective measure, e.g., the
 minimum or the average distance between a pair of points in the
 subset. Diversity maximization problems are computationally hard,
 hence only approximate solutions can be hoped for. Although its
 applications are mostly in massive data analysis, most of the past
 research on diversity maximization has concentrated on the
 standard sequential setting. Thus, there is a need for efficient
 algorithms in computational settings that can handle very large
 datasets, such as those at the base of the MapReduce and the
 Streaming models. In this work we provide algorithms for these
 models in the special case of metric spaces of bounded doubling
 dimension, which include the important family of Euclidean spaces
 of constant dimension.

If you find this software useful for your research, please cite the
paper [CeccarelloPPU16]_.

Project organization
====================

The code is organized in subprojects

- The ``core`` subproject contains the implementation of the algorithms
- The ``experiments`` subprojects contains code to run experiments on
  the algorithms

Building the software
=====================

The project is built using `sbt <http://www.scala-sbt.org/>`_. To
build all the subprojects and run the tests, use the following command::

  sbt test

or, if you prefer to skip the tests, just run::

  sbt compile

You can also build a jar containing the code to run the experiments
with all the dependencies (except for Spark and Hadoop) to be deployed
on your Spark cluster::

  sbt experiments/assembly
  
Implemented algorithms
======================

The paper [CeccarelloPPU16]_ describes algorithms in the MapReduce and
Streaming setting for several diversity maximization problems. This
repository includes an implementation in the ``core`` subproject. In
particular

- ``core/src/main/scala/it/unipd/dei/diversity/StreamingCoreset.scala``
  implements the Streaming algorithm described in Section 4 of the paper
- ``core/src/main/scala/it/unipd/dei/diversity/MapReduceCoreset.scala``
  implements the MapReduce algorithm described in Section 5 of the paper

Both implementations are parametric in both the data type and the
distance function. The ``core`` subproject contains a sample
implementation of points in a multi-dimensional space
(``core/src/main/scala/it/unipd/dei/diversity/Point.scala``) and of
some distance functions, including the Euclidean distance
(``core/src/main/scala/it/unipd/dei/diversity/Distance.scala``).

Reproducing the results
====================

.. highlight:: bash

To reproduce the experimental results in the paper, you can generate
the synthetic datasets with the following commands::

  DATASETS_DIR=hdfs://hdfs-master/path/to/datasets/directory
  
  for SIZE in 100 200 400 800 1600
  do
    spark-submit \
        --driver-library-path /path/to/hadoop/native/libs \
        --properties-file path/to/spark/config/file \
        --class it.unipd.dei.diversity.DatasetGenerator \
        diversity-maximization-experiments-assembly-0.1.0.jar \
        --source chasm-random-uniform-sphere -k 128 --directory $DATASETS_DIR -n ${SIZE}000000 --space-dimension 3
  done

As for the _musiXmatch_ dataset, which can be downloaded from `here <http://labrosa.ee.columbia.edu/millionsong/musixmatch>`_,
you can preprocess it so to obtain the one used in the paper with the following commands::

  wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/mxm_dataset_train.txt.zip
  wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/mxm_dataset_test.txt.zip
  unzip mxm_dataset_train.txt.zip
  unzip mxm_dataset_test.txt.zip
  cat mxm_dataset_train.txt mxm_dataset_test.txt > mxm.txt

  DATASETS_DIR=hdfs://hdfs-master/path/to/datasets/directory
  
  spark-submit \
      --driver-library-path /path/to/hadoop/native/libs \
      --properties-file path/to/spark/config/file \
      --class it.unipd.dei.diversity.BagOfWordsDataset \
      diversity-maximization-experiments-assembly-0.1.0.jar \
      --format mxm --input mxm.txt --output $DATASETS_DIR/mxm-bigger10.bow --transform "bigger(10)"


References
==========
  
.. [CeccarelloPPU16] Matteo Ceccarello, Andrea Pietracaprina,
   Geppino Pucci, Eli Upfal. *MapReduce and Streaming Algorithms for
   Diversity Maximization in Metric Spaces of Bounded Doubling
   Dimension* `arXiv:1605.05590 <https://arxiv.org/abs/1605.05590>`_


