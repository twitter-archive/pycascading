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


# Import frequently used Cascading classes
# We import these first so that we can override some global names (like Rename)
from cascading.tuple import Fields, Tuple, TupleEntry
from cascading.operation.aggregator import *
from cascading.operation.filter import *
from cascading.pipe.assembly import *
from cascading.scheme import *
from cascading.tap import *
from cascading.tap.hadoop import *
from cascading.scheme.hadoop import TextLine, TextDelimited

# Import all important PyCascading modules so we don't have to in the scripts
from pycascading.decorators import *
from pycascading.tap import *
from pycascading.operators import *
from pycascading.each import *
from pycascading.every import *
from pycascading.cogroup import *
# We don't import * as the name of some functions (sum) collides with Python
import pycascading.native as native

# Import Java basic types for conversions
from java.lang import Integer, Long, Float, Double, String
