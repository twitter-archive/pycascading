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

"""Used internally. PyCascading module to set up the paths for the sources.

The module that gets loaded first when a Cascading pipeline is deserialized.
PyCascading needs to start a Jython interpreter whenever a mapper or reducer
executes Python code, so we need to start an interpreter, set up the
environment, and load the job's source code.
"""

__author__ = 'Gabor Szabo'


import sys


def setup_paths(module_paths):
    """Set up sys.path on the mappers and reducers.

    module_paths is an array of path names where the sources or other
    supporting files are found. In particular, module_paths[0] is the location
    of the PyCascading Python sources, and modules_paths[1] is the location of
    the source file defining the function.

    In Hadoop mode (with remote_deploy.sh), the first two -a options must
    specify the archives of the PyCascading sources and the job sources,
    respectively.

    Arguments:
    module_paths -- the locations of the Python sources 
    """
    from com.twitter.pycascading import Util

    cascading_jar = Util.getCascadingJar()
    jython_dir = module_paths[0]

    sys.path.extend((cascading_jar, jython_dir + '/python',
                     jython_dir + '/python/Lib'))
    sys.path.extend(module_paths[1 : ])

    # Allow importing of user-installed Jython packages
    # Thanks to Simon Radford
    import site
    site.addsitedir(jython_dir + 'python/Lib/site-packages')

    # Haha... it's necessary to put this here, otherwise simplejson won't work.
    # Maybe it's automatically imported in the beginning of a Jython program,
    # but since at that point the sys.path is not set yet to Lib, it will fail?
    #import encodings
