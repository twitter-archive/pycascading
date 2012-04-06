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

* Add fields to the stream: Add
* Map fields to new fields: Map
* Select fields from the stream: Retain
* Remove fields from the stream: Discard
* Rename fields: Rename
"""

import itertools, inspect

from cascading.tuple import Fields
from cascading.operation import Identity
import cascading.pipe.assembly.Rename
import cascading.pipe.assembly.Shape

from pycascading.pipe import Apply, SubAssembly, coerce_to_fields, \
DecoratedFunction
from pycascading.decorators import udf


def _Map(output_selector, *args):
    """Maps the given input fields to output fields.

    The fields specified as input will be removed from the result.
    """
    if len(args) == 1:
        (input_selector, function, output_field) = \
        (Fields.ALL, args[0], Fields.UNKNOWN)
    elif len(args) == 2:
        if inspect.isfunction(args[0]) or isinstance(args[0], DecoratedFunction):
            # The first argument is a UDF, the second are the output fields
            (input_selector, function, output_field) = \
            (Fields.ALL, args[0], args[1])
        else:
            (input_selector, function, output_field) = \
            (args[0], args[1], Fields.UNKNOWN)
    elif len(args) == 3:
        (input_selector, function, output_field) = args
    else:
        raise Exception('Map needs to be called with 2 or 3 parameters')
    if isinstance(function, DecoratedFunction):
        # By default we take everything from the UDF's decorators
        df = function
    else:
        df = udf(produces=output_field)(function)
    return Apply(input_selector, df, output_selector)


def MapSwap(*args):
    """Map the tuple, and remove the mapped fields, and add the new fields.

    This mapping replaces the fields mapped with the new fields that the
    mapping operation adds.

    The number of arguments to this function is between 1 and 3:
    * One argument: it's the map function. The output fields will be named
      after the 'produces' parameter if the map function is decorated, or
      will be Fields.UNKNOWN if it's not defined. Note that after UNKNOW field
      names are introduced to the tuple, all the other field names are also
      lost.
    * Two arguments: it's either the input field selector and the map function,
      or the map function and the output fields' names.
    * Three arguments: they are interpreted as the input field selector, the
      map function, and finally the output fields' names.
    """
    return _Map(Fields.SWAP, *args)


def MapAdd(*args):
    """Map the defined fields (or all fields), and add the results to the tuple.

    Note that the new field names we are adding to the tuple cannot overlap
    with existing field names, or Cascading will complain.
    """
    return _Map(Fields.ALL, *args)


def Retain(*fields_to_keep):
    """Retain only the given fields.

    The fields can be given in array or by separate parameters.
    """
    if len(fields_to_keep) > 1:
        fields_to_keep = list(itertools.chain(fields_to_keep))
    else:
        fields_to_keep = fields_to_keep[0]
    return Apply(fields_to_keep, Identity(Fields.ARGS), Fields.RESULTS)


def _Discard(fields_to_discard):
    # In 2.0 there's a builtin function this, Discard
    # In 1.2 there is nothing for this
    raise Exception('Discard only works with Cascading 2.0')


def Rename(*args):
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
