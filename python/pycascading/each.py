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

"""Operations related to an Each pipe.

* Add fields to the stream: map_add
* Map fields to new fields: map_replace
* Map the whole tuple to the new tuple: map_to
* Filter tuples: filter_by
"""

__author__ = 'Gabor Szabo'


import inspect

import cascading.pipe
from cascading.tuple import Fields

from com.twitter.pycascading import CascadingFunctionWrapper, \
CascadingFilterWrapper

from pycascading.pipe import Operation, coerce_to_fields, wrap_function, \
random_pipe_name, DecoratedFunction
from pycascading.decorators import udf


class _Each(Operation):

    """The equivalent of Each in Cascading.

    We need to wrap @maps and @filters with different Java classes, but
    the constructors for Each are built similarly. This class provides this
    functionality.
    """

    def __init__(self, function_type, *args):
        """Build the Each constructor for the Python function.

        Arguments:
        function_type -- CascadingFunctionWrapper or CascadingFilterWrapper,
            whether we are calling Each with a function or filter
        *args -- the arguments passed on to Cascading Each
        """
        Operation.__init__(self)

        self.__function = None
        # The default argument selector is Fields.ALL (per Cascading sources
        # for Operator.java)
        self.__argument_selector = None
        # The default output selector is Fields.RESULTS (per Cascading sources
        # for Operator.java)
        self.__output_selector = None

        if len(args) == 1:
            self.__function = args[0]
        elif len(args) == 2:
            (self.__argument_selector, self.__function) = args
        elif len(args) == 3:
            (self.__argument_selector, self.__function,
             self.__output_selector) = args
        else:
            raise Exception('The number of parameters to Apply/Filter ' \
                            'should be between 1 and 3')
        # This is the Cascading Function type
        self.__function = wrap_function(self.__function, function_type)

    def _create_with_parent(self, parent):
        args = []
        if self.__argument_selector:
            args.append(coerce_to_fields(self.__argument_selector))
        args.append(self.__function)
        if self.__output_selector:
            args.append(coerce_to_fields(self.__output_selector))
        # We need to put another Pipe after the Each since otherwise
        # joins may not work as the names of pipes apparently have to be
        # different for Cascading.
        each = cascading.pipe.Each(parent.get_assembly(), *args)
        return cascading.pipe.Pipe(random_pipe_name('each'), each)


class Apply(_Each):
    """Apply the given user-defined function to each tuple in the stream.

    The corresponding class in Cascading is Each called with a Function.
    """
    def __init__(self, *args):
        _Each.__init__(self, CascadingFunctionWrapper, *args)


class Filter(_Each):
    """Filter the tuple stream through the user-defined function.

    The corresponding class in Cascading is Each called with a Filter.
    """
    def __init__(self, *args):
        _Each.__init__(self, CascadingFilterWrapper, *args)


def _any_instance(var, classes):
    """Check if var is an instance of any class in classes."""
    for cl in classes:
        if isinstance(var, cl):
            return True
    return False


def _map(output_selector, *args):
    """Maps the given input fields to output fields."""
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


def map_to(*args):
    """Map the tuple, and keep only the results returned by the function."""
    return _map(Fields.RESULTS, *args)


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
