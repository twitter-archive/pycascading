PyCascading is no longer maintained
===================================

PyCascading
===========

PyCascading is a Python wrapper for Cascading. You can control the
full data processing workflow from Python.

* Pipelines are built with Python operators
* User-defined functions are written in Python
* Passing arbitrary contexts to user-defined functions
* Caching of interim results in pipes for faster replay
* Uses Jython 2.5.2, easy integration with Java and Python libraries


Examples
--------

There can't be a MapReduce tutorial without counting words. Here it is:

    def main():
        ...

        @udf_map(produces=['word'])
        def split_words(tuple):
            for word in tuple.get('line').split():
                yield [word]

        input | split_words | group_by('word', native.count()) | output
        ...

Above, the user-defined function that reshapes the stream is annotated with
a PyCascading decorator, and the workflow is created by chaining operations
into each other.

More examples for the different use cases can be found in the examples folder.
See also the docstrings in the sources for a complete documentation of the
arguments.

To try the examples, first build the Java sources as described below in the
Building section. Then, change to the 'examples' folder, and issue either

../local_run.sh example.py

for a simulated Hadoop local run, or

../remote_deploy.sh -m -s hadoop_server example.py

to deploy automatically on a Hadoop server. hadoop_server is the SSH address
of an account where the master jar and script will be scp'd to. Note that the
'-m' option has to be used only once in the beginning. The '-m' option copies
the master jar to the server, and any subsequent deploys will use this master
jar, and only the actual Python script will be copied over the network.


Usage
-----

PyCascading may be used in one of two modes: in local Hadoop mode or with
remote Hadoop deployment. Please note that you need to specify the locations
of the dependencies in the java/dependencies.properties file.

In *local mode*, the script is executed in Hadoop's local mode. All files
reside on the local file system, and creating a bundled deployment jar is not
necessary.

To run in this mode, use the script *local_run.sh*, with the first parameter
being the PyCascading script. Additional command line parameters may be used
to pass on to the script.

In *Hadoop mode*, we assume that Hadoop runs on a remote SSH server (or
localhost). First, a master jar is built and copied to the server. This jar
contains all the PyCascading classes and other dependencies (but not Hadoop)
needed to run a job, and may get rather large if there are a few external jars
included. For this reason it is copied to the Hadoop deployment server only
once, and whenever a new PyCascading script is run by the user, only the
Pythn script is copied to the remote server and bundled there for submission
to Hadoop. The first few variables in the remote_deploy.sh script specify
the Hadoop server and the folders where the deployment files should be placed. 

Use the remote_deploy.sh script to deploy a PyCascading script to the remote
Hadoop server.


Building
--------

Requirements for building:

* Cascading 1.2.* or 2.0.0 (http://www.concurrentinc.com/downloads/)
* Jython 2.5.2+ (http://www.jython.org/downloads.html)
* Hadoop 0.20.2+, the version preferably matching the Hadoop runtime
(http://www.apache.org/dyn/closer.cgi/hadoop/common/)
* A Java compiler
* Ant (http://ant.apache.org/)

Requirements for running:

* Hadoop installed and set up on the target server (http://hadoop.apache.org/)
* SSH access to the remote server
* If testing scripts locally, a reasonable JVM callable by "java"

PyCascading consists of Java and Python sources. Python sources need no
compiling, but the Java part needs to be built with Ant. For this, change to
the 'java' folder, and invoke ant. This should build the sources and create
a master jar for job submission.

The locations of the Jython, Cascading, and Hadoop folders on the file system
are specified in the java/dependencies.properties file. You need to correctly
specify these before compiling the source.

Also, check the remote_deploy.sh script and the locations defined in the
beginning of that file on where to put the jar files on the Hadoop server.


Bugs
----

Have a bug or feature request? Please create an issue here on GitHub!

https://github.com/twitter/pycascading/issues


Mailing list
------------

Currently we are using the cascading-user mailing list for discussions. Any
questions, please ask there.

http://groups.google.com/group/cascading-user


Authors
-------

**Gabor Szabo**

+ http://twitter.com/gaborjszabo

License
---------------------

Copyright 2011 Twitter, Inc.

Licensed under the Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0
