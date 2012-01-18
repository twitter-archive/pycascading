#
# Copyright 2011 Twitter, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Bootstrap the PyCascading script.

It must be called with the first command line parameter being 'local' or
'hadoop'. This determines whether we're running the script in local mode or
with Hadoop. For Hadoop we need to pack the sources into a jar, which are
extracted later to a temporary directory, so we need to set up the search
paths differently in this case.
"""

__author__ = 'Gabor Szabo'


import sys, imp


if __name__ == "__main__":
    # The first command line parameter must be 'hadoop' or 'local'
    # to indicate the running mode
    running_mode = sys.argv[1]

    from com.twitter.pycascading import Util

    cascading_jar = Util.getCascadingJar()
    # This is the folder where Hadoop extracted the jar file for execution
    tmp_dir = Util.getJarFolder()
    sys.path.extend((cascading_jar, '.', tmp_dir, tmp_dir + '/python',
                     tmp_dir + '/python/Lib'))

    # Haha... it's necessary to put this here, otherwise simplejson won't work.
    # Maybe it's automatically imported in the beginning of a Jython program,
    # but since at that point the sys.path is not set yet to Lib, it will fail?
    # Instead, we can use Java's JSON decoder...
#    import encodings

    m = imp.load_source('main', sys.argv[2])
    # We need to explicitly inject running_mode into the tap module,
    # otherwise we cannot import bootstrap from tap and use the
    # bootstrap.running_mode from here.
    import pycascading.pipe
    pycascading.pipe.running_mode = running_mode

    # Remove the running mode argument so that sys.argv will look like as
    # if it was coming from a simple command line execution
    del sys.argv[0 : 2]

    m.main()
