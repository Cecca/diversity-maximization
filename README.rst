========================
 Diversity maximization
========================

.. image:: https://travis-ci.org/Cecca/diversity-maximization.svg?branch=master
   :target: https://travis-ci.org/Cecca/diversity-maximization

This repository contains code implementing the algorithms presented in
*A General Coreset-Based Approach to Diversity Maximization under Matroid Constraints*. From the abstract of the paper

 For a given dataset S of n elements, the problem requires to determine a subset of S containing k≪n “representatives” which maximize some diversity function expressed in terms of pairwise distances, where distance models dissimilarity. An important variant of the problem prescribes that the solution satisfy an additional orthogonal requirement, which can be specified as a matroid constraint (i.e., a feasible solution must be an independent set of size k of a given matroid). 

If you find this software useful for your research, please cite the
following papers::


    @article{DBLP:journals/tkdd/CeccarelloPP20,
      author    = {Matteo Ceccarello and
                   Andrea Pietracaprina and
                   Geppino Pucci},
      title     = {A General Coreset-Based Approach to Diversity Maximization under Matroid
                   Constraints},
      journal   = {{ACM} Trans. Knowl. Discov. Data},
      volume    = {14},
      number    = {5},
      pages     = {60:1--60:27},
      year      = {2020},
      url       = {https://dl.acm.org/doi/10.1145/3402448},
      timestamp = {Wed, 02 Sep 2020 13:05:07 +0200},
      biburl    = {https://dblp.org/rec/journals/tkdd/CeccarelloPP20.bib},
      bibsource = {dblp computer science bibliography, https://dblp.org}
    }
    
    @inproceedings{DBLP:conf/wsdm/CeccarelloPP18,
      author    = {Matteo Ceccarello and
                   Andrea Pietracaprina and
                   Geppino Pucci},
      editor    = {Yi Chang and
                   Chengxiang Zhai and
                   Yan Liu and
                   Yoelle Maarek},
      title     = {Fast Coreset-based Diversity Maximization under Matroid Constraints},
      booktitle = {Proceedings of the Eleventh {ACM} International Conference on Web
                   Search and Data Mining, {WSDM} 2018, Marina Del Rey, CA, USA, February
                   5-9, 2018},
      pages     = {81--89},
      publisher = {{ACM}},
      year      = {2018},
      url       = {https://doi.org/10.1145/3159652.3159719},
      doi       = {10.1145/3159652.3159719},
      timestamp = {Thu, 13 Aug 2020 18:13:38 +0200},
      biburl    = {https://dblp.org/rec/conf/wsdm/CeccarelloPP18.bib},
      bibsource = {dblp computer science bibliography, https://dblp.org}
    }


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



