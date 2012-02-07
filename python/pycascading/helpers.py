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

"""Helper functions for a PyCascading script.

This module imports the PyCascading modules so that we don't have to import
them manually all the time. It also imports the Java classes wrapping the
primitive types (Integer, Long, Float, Double, String), so that casts are made
easy. Furthermore frequently used Cascading classes are also imported, such as
Fields, Tuple, and TupleEntry, and the pre-defined aggregators, filters,
assemblies, and schemes.
"""

__author__ = 'Gabor Szabo'


import time, struct, subprocess


# Import all important PyCascading modules so we don't have to in the scripts
from pycascading.decorators import *
from pycascading.pipe import *
from pycascading.tap import *

# Import Java basic types for conversions
from java.lang import Integer, Long, Float, Double, String

# Import frequently used Cascading classes
from cascading.tuple import Fields, Tuple, TupleEntry
from cascading.operation.aggregator import *
from cascading.operation.filter import *
from cascading.pipe.assembly import *
from cascading.scheme import *
from cascading.tap import *

import com.twitter.pycascading.SelectFields
from pycascading.pipe import coerce_to_fields


class Getter():

    """A wrapper for an object with 'get' and 'set' methods.

    If the object has a .get(key) method and a .set(key, value) method,
    these can be replaced by referencing the key with []s.
    """

    def __init__(self, object):
        self.object = object

    def __getitem__(self, key):
        return self.object.get(key)

    def __setitem__(self, key, value):
        return self.object.set(key, value)


def time2epoch(t):
    """Converts times in UTC to seconds since the UNIX epoch, 1/1/1970 00:00.

    Arguments:
    t -- the time string in 'YYYY-MM-DD hh:mm:ss' format

    Exceptions:
    Throws an exception if t is not in the right format.
    """
    t = time.strptime(t + ' UTC', '%Y-%m-%d %H:%M:%S.0 %Z')
    return int(time.mktime(t)) - time.timezone


def bigendian2long(b):
    """Converts a series of 4 bytes in big-endian format to a Java Long.

    Arguments:
    b -- a string of 4 bytes that represent a word
    """
    return Long(struct.unpack('>I', b)[0])


def bigendian2int(b):
    """Converts a series of 4 bytes in big-endian format to a Python int.

    Arguments:
    b -- a string of 4 bytes that represent a word
    """
    return struct.unpack('>i', b)[0]


def SelectFields(fields):
    """Keeps only some fields in the tuple stream.

    Arguments:
    fields -- a list of fields to keep, or a Cascading Fields wildcard
    """
    return com.twitter.pycascading.SelectFields(coerce_to_fields(fields))


def read_hdfs_tsv_file(path):
    """Read a tab-separated HDFS folder and yield the records.

    The first line of the file should contain the name of the fields. Each
    record contains columns separated by tabs.

    Arguments:
    path -- path to a tab-separated folder containing the data files
    """
    pipe = subprocess.Popen('hdfs -cat "%s/.pycascading_header" "%s/part-*"' \
    % (path, path), shell=True, stdout=subprocess.PIPE).stdout
    first_line = True
    for line in pipe:
        line = line[0 : (len(line) - 1)]
        fields = line.split('\t')
        if first_line:
            field_names = fields
            first_line = False
        else:
            yield dict(zip(field_names, fields))
