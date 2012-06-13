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

"""Operations related to an Every pipe."""

__author__ = 'Gabor Szabo'


import inspect

import cascading.pipe
import cascading.operation
from cascading.tuple import Fields

from com.twitter.pycascading import CascadingAggregatorWrapper, \
CascadingBufferWrapper

from pycascading.pipe import Operation, coerce_to_fields, wrap_function, \
random_pipe_name, DecoratedFunction, _Stackable

from pycascading.decorators import udf


class Every(Operation):

    """Apply an operation to a group of tuples.

    This operation is similar to Apply, but can only follow a GroupBy or
    CoGroup. It runs a Cascading Aggregator or Buffer on every grouping.
    Native Java aggregators or buffers may be used, and also PyCascading
    @reduces.

    By default the tuples contain only the values in a group, but not the
    grouping field. This can be had from the group first parameter.
    """

    def __init__(self, *args, **kwargs):
        """Create a Cascading Every pipe.

        Keyword arguments:
        aggregator -- a Cascading aggregator (only either aggregator or buffer
            should be used)
        buffer -- a Cascading Buffer or a PyCascading @reduce function
        output_selector -- the outputSelector parameter for Cascading
        argument_selector -- the argumentSelector parameter for Cascading
        assertion_level -- the assertionLevel parameter for Cascading
        assertion -- the assertion parameter for Cascading
        """
        Operation.__init__(self)
        self.__args = args
        self.__kwargs = kwargs

    def __create_args(self,
                      pipe=None,
                      aggregator=None, output_selector=None,
                      assertion_level=None, assertion=None,
                      buffer=None,
                      argument_selector=None):
        if self.__args:
            # If we pass in an unnamed argument, try to determine its type
            if isinstance(self.__args[0], cascading.operation.Aggregator):
                aggregator = self.__args[0]
            else:
                buffer = self.__args[0]
        # Set up some defaults
        if argument_selector is None:
            argument_selector = cascading.tuple.Fields.ALL
        if output_selector is None:
            if aggregator is not None:
                # In the case of aggregators, we want to return both the
                # groupings and the results
                output_selector = cascading.tuple.Fields.ALL
            else:
                output_selector = cascading.tuple.Fields.RESULTS

        args = []
        args.append(pipe.get_assembly())
        if argument_selector is not None:
            args.append(coerce_to_fields(argument_selector))
        if aggregator is not None:
            # for now we assume it's a Cascading aggregator straight
            args.append(wrap_function(aggregator, CascadingAggregatorWrapper))
            if output_selector:
                args.append(coerce_to_fields(output_selector))
        if assertion_level is not None:
            args.append(assertion_level)
            args.append(assertion)
        if buffer is not None:
            args.append(wrap_function(buffer, CascadingBufferWrapper))
            if output_selector:
                args.append(coerce_to_fields(output_selector))
        return args

    def _create_with_parent(self, parent):
        args = self.__create_args(pipe=parent, **self.__kwargs)
        return cascading.pipe.Every(*args)


class GroupBy(Operation):

    """GroupBy first merges the given pipes, then groups by the fields given.

    This class does the same as the corresponding Cascading GroupBy.
    """

    def __init__(self, *args, **kwargs):
        """Create a Cascading Every pipe.

        Arguments:
        args[0] -- the fields on which to group

        Keyword arguments:
        group_name -- the groupName parameter for Cascading
        group_fields -- the fields on which to group
        sort_fields -- the sortFields parameter for Cascading
        reverse_order -- the reverseOrder parameter for Cascading
        lhs_pipe -- the lhsPipe parameter for Cascading
        rhs_pipe -- the rhsPipe parameter for Cascading
        """
        Operation.__init__(self)
        self.__args = args
        self.__kwargs = kwargs

    def __create_args(self,
                      group_name=None,
                      pipes=None, group_fields=None, sort_fields=None,
                      reverse_order=None,
                      pipe=None,
                      lhs_pipe=None, rhs_pipe=None):
        # We can use an unnamed parameter only for group_fields
        if self.__args:
            group_fields = coerce_to_fields(self.__args[0])
        args = []
        if group_name:
            args.append(group_name)
        if pipes:
            args.append([p.get_assembly() for p in pipes])
            if group_fields:
                args.append(coerce_to_fields(group_fields))
                if sort_fields:
                    args.append(coerce_to_fields(sort_fields))
                    if reverse_order:
                        args.append(reverse_order)
        elif pipe:
            args.append(pipe.get_assembly())
            if group_fields:
                args.append(coerce_to_fields(group_fields))
                if sort_fields:
                    args.append(coerce_to_fields(sort_fields))
                if reverse_order:
                    args.append(reverse_order)
        elif lhs_pipe:
            args.append(lhs_pipe.get_assembly())
            args.append(rhs_pipe.get_assembly())
            args.append(coerce_to_fields(group_fields))
        return args

    def _create_with_parent(self, parent):
        if isinstance(parent, _Stackable):
            # We're chaining with a _Stackable object
            args = self.__create_args(pipes=parent.stack, **self.__kwargs)
        else:
            # We're chaining with a Chainable object
            args = self.__create_args(pipe=parent, **self.__kwargs)
        return cascading.pipe.GroupBy(*args)


class _DelayedInitialization(Operation):
    def __init__(self, callback):
        Operation.__init__(self)
        self.__callback = callback

    def _create_with_parent(self, parent):
        return self.__callback(parent).get_assembly()


def group_by(*args, **kwargs):
    if len(args) == 0:
        grouping_fields = None
        parameters = ()
    elif len(args) == 1:
        grouping_fields = args[0]
        parameters = ()
    elif len(args) == 2:
        grouping_fields = args[0]
        parameters = (Fields.ALL, args[1], Fields.UNKNOWN)
    elif len(args) == 3:
        grouping_fields = args[0]
        if inspect.isfunction(args[1]) or isinstance(args[1], \
        (DecoratedFunction, cascading.operation.Aggregator, cascading.operation.Buffer)):
            # The first argument is an aggregator/buffer,
            # the second is the output fields
            parameters = (Fields.ALL, args[1], args[2])
        else:
            parameters = (args[1], args[2], Fields.UNKNOWN)
    elif len(args) == 4:
        grouping_fields = args[0]
        parameters = (args[1], args[2], args[3])
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
            if grouping_fields:
                return parent | GroupBy(grouping_fields, **kwargs) | \
                    Every(df, argument_selector=input_selector)
            else:
                return parent | GroupBy(**kwargs) | \
                    Every(df, argument_selector=input_selector)
        return _DelayedInitialization(pipe)
    else:
        def pipe(parent):
            if grouping_fields:
                return parent | GroupBy(grouping_fields, **kwargs)
            else:
                return parent | GroupBy(**kwargs)
        return _DelayedInitialization(pipe)
