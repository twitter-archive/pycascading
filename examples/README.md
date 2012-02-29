PyCascading examples
====================

This folder showcases a number of features offered by Cascading and
PyCascading. They use input files in the 'pycascading\_data' folder, so
before running the examples, makes sure that:

* in local mode, you cd first to the examples/ directory (or wherever
pycascading\_data/ is found), and use local\_run.sh
* in Hadoop mode, you copy the data folder to HDFS first by running
copy\_data\_to\_hdfs.sh, or

	hadoop fs -put pycascading\_data pycascading\_data

  and then invoke remove\_deploy.sh
