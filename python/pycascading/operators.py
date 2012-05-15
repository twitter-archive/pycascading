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

"""Various operations acting on the tuples.

* Select fields from the stream: retain
* Remove fields from the stream: discard (not implemented in Cascading 1.2.*)
* Rename fields: rename
"""

__author__ = 'Gabor Szabo'


import itertools

from cascading.tuple import Fields
from cascading.operation import Identity
import cascading.pipe.assembly.Rename

from pycascading.pipe import SubAssembly, coerce_to_fields
from pycascading.each import Apply


def retain(*fields_to_keep):
    """Retain only the given fields.

    The fields can be given in array or by separate parameters.
    """
    if len(fields_to_keep) > 1:
        fields_to_keep = list(itertools.chain(fields_to_keep))
    else:
        fields_to_keep = fields_to_keep[0]
    return Apply(fields_to_keep, Identity(Fields.ARGS), Fields.RESULTS)


def _discard(fields_to_discard):
    # In 2.0 there's a builtin function this, Discard
    # In 1.2 there is nothing for this
    raise Exception('Discard only works with Cascading 2.0')


def rename(*args):
    """Rename the fields to new names.

    If only one argument (a list of names) is given, it is assumed that the
    user wants to rename all the fields. If there are two arguments, the first
    list is the set of fields to be renamed, and the second is a list of the
    new names.
    """
    if len(args) == 1:
        (fields_from, fields_to) = (Fields.ALL, args[0])
    else:
        (fields_from, fields_to) = (args[0], args[1])
    return SubAssembly(cascading.pipe.assembly.Rename, \
                       coerce_to_fields(fields_from), \
                       coerce_to_fields(fields_to))
