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

"""Taps (sources and sinks) in PyCascading.

All taps need to be registered using this module because Cascading expects
them to be named by strings when running the flow.

Exports the following:
Flow
read_hdfs_tsv_file
"""

__author__ = 'Gabor Szabo'


from pycascading.pipe import random_pipe_name, Chainable, Pipe
from com.twitter.pycascading import Util, MetaScheme

import cascading.tap
import cascading.scheme
from cascading.tuple import Fields

from org.apache.hadoop.fs import Path
from org.apache.hadoop.conf import Configuration

from pipe import random_pipe_name, Operation


def expand_path_with_home(output_folder):
    """Prepend the home folder to a relative location on HDFS if necessary.

    Only if we specified a relative path and no scheme, prepend it with the
    home folder of the user on HDFS. This behavior is similar to how
    "hadoop fs" works. If we are running in local mode, don't do anything.

    Arguments:
    output_folder -- the absolute or relative path of the output HDFS folder
    """
    import pycascading.pipe
    if pycascading.pipe.config['pycascading.running_mode'] == 'hadoop':
        if not any(map(lambda scheme: output_folder.startswith(scheme), \
                       ['hdfs:', 'file:', 's3:', 's3n:', '/'])):
            fs = Path('/').getFileSystem(Configuration())
            home_folder = fs.getHomeDirectory().toString()
            return home_folder + '/' + output_folder
    return output_folder


class Flow(object):

    """Define sources and sinks for the flow.

    This associates all sources and sinks with their head pipe mappings.
    The default number of reducers is 100. Set this in the num_reducers
    parameter when starting the flow with run().
    """

    def __init__(self):
        self.source_map = {}
        self.sink_map = {}
        self.tails = []

    def _connect_source(self, pipe_name, cascading_tap):
        """Add a source to the flow.

        Cascading needs to map taps to a pipeline with string names. This is
        inconvenient, but we need to keep track of these strings. We also need
        to count references to taps, as sometimes we need to remove pipelines
        due to replacement with a cache, and in this case we may also need to
        remove a tap. Otherwise Cascading complains about not all
        taps/pipelines being connected up to the flow.
        """
        self.source_map[pipe_name] = cascading_tap

    def source(self, cascading_tap):
        """A generic source using Cascading taps.

        Arguments:
        cascading_tap -- the Cascading Scheme object to store data into
        """
        # We can create the source tap right away and also use a Pipe to name
        # the head of this pipeline
        p = Pipe(name=random_pipe_name('source'))
        p.hash = hash(cascading_tap)
        p.add_context([p.get_assembly().getName()])
        self._connect_source(p.get_assembly().getName(), cascading_tap)
        return p

    def meta_source(self, input_path):
        """Use data files in a folder and read the scheme from the meta file.

        Defines a source tap using files in input_path, which should be a
        (HDFS) folder. Takes care of using the appropriate scheme that was
        used to store the data, using meta data in the data folder.

        Arguments:
        input_path -- the HDFS folder to store data into
        """
        input_path = expand_path_with_home(input_path)
        source_scheme = MetaScheme.getSourceScheme(input_path)
        return self.source(cascading.tap.hadoop.Hfs(source_scheme, input_path))

    def sink(self, cascading_scheme):
        """A Cascading sink using a Cascading Scheme.

        Arguments:
        cascading_scheme -- the Cascading Scheme used to store the data
        """
        return _Sink(self, cascading_scheme)

    def meta_sink(self, cascading_scheme, output_path):
        """Store data together with meta information about the scheme used.

        A sink that also stores in a file information about the scheme used to
        store data, and human-readable descriptions in the .pycascading_header
        and .pycascading_types files with the field names and their types,
        respectively.

        Arguments:
        cascading_scheme -- the Cascading Scheme used to store data
        output_path -- the folder where the output tuples should be stored.
            If it exists, it will be erased and replaced!
        """
        output_path = expand_path_with_home(output_path)
        sink_scheme = MetaScheme.getSinkScheme(cascading_scheme, output_path)
        return self.sink(cascading.tap.hadoop.Hfs(sink_scheme, output_path,
                                           cascading.tap.SinkMode.REPLACE))

    def tsv_sink(self, output_path, fields=Fields.ALL):
        # TODO: in local mode, do not prepend the home folder to the path
        """A sink to store the tuples as tab-separated values in text files.

        Arguments:
        output_path -- the folder for the output
        fields -- the fields to store. Defaults to all fields.
        """
        output_path = expand_path_with_home(output_path)
        return self.meta_sink(cascading.scheme.hadoop.TextDelimited(fields, '\t'),
                              output_path)

    def binary_sink(self, output_path, fields=Fields.ALL):
        """A sink to store binary sequence files to store the output.

        This is a sink that uses the efficient Cascading SequenceFile scheme to
        store data. This is a serialized version of all tuples and is
        recommended when we want to store intermediate results for fast access
        later.

        Arguments:
        output_path -- the (HDFS) folder to store data into
        fields -- the Cascading Fields field selector of which tuple fields to
            store. Defaults to Fields.ALL.
        """
        output_path = expand_path_with_home(output_path)
        return self.meta_sink(cascading.scheme.hadoop.SequenceFile(fields),
                              output_path)

    def cache(self, identifier, refresh=False):
        """A sink for temporary results.

        This caches results into a temporary folder if the folder does not
        exist yet. If we need to run slightly modified versions of the
        PyCascading script several times during testing for instance, this is
        very useful to store some results that can be reused without having to
        go through the part of the flow that generated them again.

        Arguments:
        identifier -- the unique identifier for this cache. This is used as
            part of the path where the temporary files are stored.
        refresh -- if True, we will regenerate the cache data as if it was
            the first time creating it
        """
        return _Cache(self, identifier, refresh)

    def run(self, num_reducers=50, config=None):
        """Start the Cascading job.

        We call this when we are done building the pipeline and explicitly want
        to start the flow process.
        """
        sources_used = set([])
        for tail in self.tails:
            sources_used.update(tail.context)
        # Remove unused sources from the source map
        source_map = {}
        for source in self.source_map.iterkeys():
            if source in sources_used:
                source_map[source] = self.source_map[source]
        tails = [t.get_assembly() for t in self.tails]
        import pycascading.pipe
        Util.run(num_reducers, pycascading.pipe.config, source_map, \
                 self.sink_map, tails)


class _Sink(Chainable):

    """A PyCascading sink that can be used as the tail in a pipeline.

    Used internally.
    """

    def __init__(self, taps, cascading_tap):
        Chainable.__init__(self)
        self.__cascading_tap = cascading_tap
        self.__taps = taps

    def _create_with_parent(self, parent):
        # We need to name every tail differently so that Cascading can assign
        # a tail map to all sinks.
        # TODO: revise this after I name every pipe part separately
        parent = parent | Pipe(name=random_pipe_name('sink'))
        self.__taps.sink_map[parent.get_assembly().getName()] = \
        self.__cascading_tap
        self.__taps.tails.append(parent)
        return None


class _Cache:

    """Act as a source or sink to store and retrieve temporary data."""

    def __init__(self, taps, hdfs_folder, refresh=False):
        tmp_folder = 'pycascading.cache/' + hdfs_folder
        self.__cache_folder = expand_path_with_home(tmp_folder)
        self.__hdfs_folder_exists = \
        self.hdfs_folder_exists(self.__cache_folder)
        self.__taps = taps
        self.__refresh = refresh

    def hdfs_folder_exists(self, folder):
        path = Path(folder)
        fs = path.getFileSystem(Configuration())
        try:
            status = fs.getFileStatus(path)
            # TODO: there could be problems if it exists but is a simple file
            return status.isDir()
        except:
            return False

    def __or__(self, pipe):
        if not self.__refresh and self.__hdfs_folder_exists:
            # We remove all sources that are replaced by this cache, otherwise
            # Cascading complains about unused source taps
            return self.__taps.meta_source(self.__cache_folder)
        else:
            # We split the data into storing and processing pipelines
            pipe | Pipe(random_pipe_name('cache')) | \
            self.__taps.binary_sink(self.__cache_folder)
            return pipe | Pipe(random_pipe_name('no_cache'))
