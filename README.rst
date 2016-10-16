========================
 Diversity maximization
========================

{<img src="https://travis-ci.org/Cecca/diversity-maximization.svg?branch=master" alt="Build Status" />}[https://travis-ci.org/Cecca/diversity-maximization]

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

  sbt core/test experiments/test

or, if you prefer to skip the tests, just run::

  sbt core/compile experiments/compile
  
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

  
References
==========
  
.. [CeccarelloPPU16] Matteo Ceccarello, Andrea Pietracaprina,
   Geppino Pucci, Eli Upfal. *MapReduce and Streaming Algorithms for
   Diversity Maximization in Metric Spaces of Bounded Doubling
   Dimension* `arXiv:1605.05590 <https://arxiv.org/abs/1605.05590>`_


