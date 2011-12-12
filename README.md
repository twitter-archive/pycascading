PyCascading
===========

PyCascading is a Python wrapper for Cascading. You can control the full data
processing workflow from Python.

* Pipelines are built with Python operators
* User-defined functions are written in Python
* Passing arbitrary contexts to user-defined functions
* Caching of interim results in pipes for faster replay
* Uses Jython 2.5.2, easy integration with Java and Python libraries


Examples
--------

There can't be a big data tutorial without counting words. Here it is:

	@map(produces=['word'])
	def split_words(tuple):
    	for word in tuple.get(1).split():
        	yield [word]

	def main():
		...
    	input | split_words | GroupBy('word') | Count() | output
		...

Above, the user-defined function that reshapes the stream is annotated with
a PyCascading decorator, and the workflow is created by chaining operations
into each other.

More examples for the different use cases can be found in the examples folder.
See also the the docstrings in the sources for a complete documentation of the
arguments.


Usage
-----

PyCascading may be used in one of two modes: in local Hadoop mode or with
remote Hadoop deployment.

In *local mode*, the script is executed in Hadoop's local mode. All files
reside on the local file system, and creating a bundled deployment jar is not
necessary.

To run in this mode, use the script local_run.sh, with the first parameter
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


Building
--------

Requirements:

* Cascading 1.2.* or 2.0.0 (http://www.concurrentinc.com/downloads/)
* Jython 2.5.2+ (http://www.jython.org/downloads.html)
* Hadoop 0.20.2+ (http://www.apache.org/dyn/closer.cgi/hadoop/common/)

PyCascading consists of Java and Python sources. Python sources need no
compiling, but the Java part needs to be built with Ant. For this, change to
the 'java' folder, and invoke ant. This should build the sources and create
a master jar for job submission.

The locations of the Jython, Cascading, and Hadoop folders on the file system
are specified in the java/dependencies.properties file. You need to correctly
specify these before compiling the source.

Also, check the remote_deploy.sh script and the locations defined in the
beginning of that file on where to put the jar files on the Hadoop server.


Versioning
----------

For transparency and insight into our release cycle, and for striving to
maintain backwards compatibility, pycascading will be maintained under the
semantic versioning guidelines as much as possible. Releases will be numbered
with the follow format:

`<major>.<minor>.<patch>`

And constructed with the following guidelines:

* Breaking backwards compatibility bumps the major
* New additions without breaking backwards compatibility bumps the minor
* Bug fixes and misc changes bump the patch

For more information on semantic versioning, please visit http://semver.org/.


Bugs
----

Have a bug or feature request? Please create an issue here on GitHub!

https://github.com/twitter/pycascading/issues


Mailing list
------------

Have a question? Ask on our mailing list!

pycascading@googlegroups.com

http://groups.google.com/group/pycascading


Authors
-------

**Gabor Szabo**

+ http://twitter.com/gaborjszabo

License
---------------------

Copyright 2011 Twitter, Inc.

Licensed under the Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0
