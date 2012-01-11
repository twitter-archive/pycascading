#!/usr/bin/env bash

# Run this on the Hadoop server to copy the data files needed
# to run the PyCascading examples to HDFS
hadoop fs -put pycascading_data pycascading_data
