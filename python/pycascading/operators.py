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

* Add fields to the stream: map_add
* Map fields to new fields: map_remove
* Select fields from the stream: Retain
* Remove fields from the stream: Discard (not implemented in Cascading 1.2.*)
* Rename fields: Rename
"""

import itertools, inspect

from cascading.tuple import Fields
from cascading.operation import Identity, Function, Filter, Aggregator, Buffer
import cascading.pipe.assembly.Rename
import cascading.pipe.assembly.Shape

from pycascading.each import Apply, Filter
from pycascading.every import Every, GroupBy
from pycascading.pipe import Operation, SubAssembly, coerce_to_fields, \
DecoratedFunction
from pycascading.decorators import udf


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


def _any_instance(var, classes):
    """Check if var is an instance of any class in classes."""
    for cl in classes:
        if isinstance(var, cl):
            return True
    return False


def _map(output_selector, *args):
    """Maps the given input fields to output fields.

    The fields specified as input will be removed from the result.
    """
    if len(args) == 1:
        (input_selector, function, output_field) = \
        (Fields.ALL, args[0], Fields.UNKNOWN)
    elif len(args) == 2:
        if inspect.isfunction(args[0]) or _any_instance(args[0], \
        (DecoratedFunction, cascading.operation.Function, cascading.operation.Filter)):
            # The first argument is a function, the second is the output fields
            (input_selector, function, output_field) = \
            (Fields.ALL, args[0], args[1])
        else:
            # The first argument is the input tuple argument selector,
            # the second one is the function
            (input_selector, function, output_field) = \
            (args[0], args[1], Fields.UNKNOWN)
    elif len(args) == 3:
        (input_selector, function, output_field) = args
    else:
        raise Exception('map_{add,replace} needs to be called with 1 to 3 parameters')
    if isinstance(function, DecoratedFunction):
        # By default we take everything from the UDF's decorators
        df = function
        if output_field != Fields.UNKNOWN:
            # But if we specified the output fields for the map, use that
            df = DecoratedFunction.decorate_function(function.decorators['function'])
            df.decorators = dict(function.decorators)
            df.decorators['produces'] = output_field
    elif inspect.isfunction(function):
        df = udf(produces=output_field)(function)
    else:
        df = function
    return Apply(input_selector, df, output_selector)


def map_add(*args):
    """Map the defined fields (or all fields), and add the results to the tuple.

    Note that the new field names we are adding to the tuple cannot overlap
    with existing field names, or Cascading will complain.
    """
    return _map(Fields.ALL, *args)


def map_replace(*args):
    """Map the tuple, remove the mapped fields, and add the new fields.

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
    return _map(Fields.SWAP, *args)


def filter_by(function):
    if isinstance(function, DecoratedFunction):
        # We make sure we will treat the function as a filter
        # Here we make a copy of the decorators so that we don't overwrite
        # the original parameters
        if function.decorators['type'] not in ('filter', 'auto'):
            raise Exception('Function is not a filter')
        df = DecoratedFunction.decorate_function(function.decorators['function'])
        df.decorators = dict(function.decorators)
        df.decorators['type'] = 'filter'
    else:
        df = udf(type='filter')(function)
    return Filter(df)


class _DelayedInitialization(Operation):
    def __init__(self, callback):
        Operation.__init__(self)
        self.__callback = callback

    def _create_with_parent(self, parent):
        return self.__callback(parent).get_assembly()


def group_by(grouping_fields, *args, **kwargs):
    if len(args) == 0:
        parameters = ()
    elif len(args) == 1:
        parameters = (Fields.ALL, args[0], Fields.UNKNOWN)
    elif len(args) == 2:
        if inspect.isfunction(args[0]) or isinstance(args[0], \
        (DecoratedFunction, cascading.operation.Aggregator, cascading.operation.Buffer)):
            # The first argument is an aggregator/buffer,
            # the second is the output fields
            parameters = (Fields.ALL, args[0], args[1])
        else:
            parameters = (args[0], args[1], Fields.UNKNOWN)
    elif len(args) == 3:
        parameters = args
    else:
        raise Exception('group_by needs to be called with 1 to 4 parameters')

    if parameters:
        (input_selector, function, output_field) = parameters
        if isinstance(function, DecoratedFunction):
            # By default we take everything from the UDF's decorators
            df = function
            if output_field != Fields.UNKNOWN:
                # But if we specified the output fields for the map, use that
                df = DecoratedFunction.decorate_function(function.decorators['function'])
                df.decorators = dict(function.decorators)
                df.decorators['produces'] = output_field
        elif inspect.isfunction(function):
            df = udf(produces=output_field)(function)
        else:
            df = function
        def pipe(parent):
            return parent | GroupBy(grouping_fields, **kwargs) | \
                Every(df, argument_selector=input_selector, \
                      output_selector=Fields.ALL)
        return _DelayedInitialization(pipe)
    else:
        def pipe(parent):
            return parent | GroupBy(grouping_fields, **kwargs)
        return _DelayedInitialization(pipe)
