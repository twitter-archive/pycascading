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

"""Build and execute Cascading pipelines in Python.

Exports the following:
Pipe
Apply
Every
GroupBy
CoGroup
Join
OuterJoin
LeftOuterJoin
RightOuterJoin
SubAssembly
coerce_to_fields
random_pipe_name
"""

__author__ = 'Gabor Szabo'


import types

import cascading.pipe
import cascading.tuple
import cascading.operation
import cascading.pipe.cogroup
from com.twitter.pycascading import CascadingFunctionWrapper, \
CascadingFilterWrapper, CascadingAggregatorWrapper, CascadingBufferWrapper, \
PythonFunctionWrapper

import java.lang.Integer


def coerce_to_fields(obj):
    """
    Utility function to convert a list or field name to cascading.tuple.Fields.

    Arguments:
    obj -- a cascading.tuple.Fields, an integer, or a string, or a list of
        integers and/or strings identifying fields

    Return:
    obj coerced to a cascading.tuple.Fields object
    """
    if isinstance(obj, list):
        # For some reason integers will not be cast to Comparables by Jython,
        # so we need to do it manually before calling the Fields constructor
        for i in xrange(len(obj)):
            if isinstance(obj[i], int):
                obj[i] = java.lang.Integer(obj[i])
        return cascading.tuple.Fields(obj)
    elif isinstance(obj, str) or isinstance(obj, int):
        if isinstance(obj, int):
            obj = java.lang.Integer(obj)
        return cascading.tuple.Fields([obj])
    else:
        # obj is assumed to be Fields already
        return obj


def random_pipe_name(prefix):
    """Generate a random string that can be used to name pipes.

    Otherwise Cascading always gets confused.
    """
    import random, re, traceback
    stack = traceback.extract_stack()
    stack.reverse()
    file = None
    for s in stack:
        if not re.match(r'.*/pycascading/[^/]+\.py$', s[0]) and \
        not re.match(r'.*/bootstrap.py$', s[0]):
            file = s[0]
            line = s[1]
            i = file.rfind('/')
            if i >= 0:
                file = file[i + 1 :]
            break
    name = prefix
    if file:
        name = name + '/' + str(line) + ':' + file
    name += ' '
    id = ''
    for i in xrange(0, 4):
        name += chr(random.randint(ord('a'), ord('z')))
    return name


def _python_function_to_java(function):
    """Create the serializable Java object for a Python function."""
    wrapped_func = PythonFunctionWrapper(function)
    if running_mode == 'local':
        wrapped_func.setRunningMode(PythonFunctionWrapper.RunningMode.LOCAL)
    else:
        wrapped_func.setRunningMode(PythonFunctionWrapper.RunningMode.HADOOP)
    return wrapped_func


def _wrap_function(function, casc_function_type):
    """Wrap a Python function into a Serializable and callable Java object.
    This wrapping is necessary as Cascading serializes the job pipeline before
    it sends the job to the workers. We need to in essence reconstruct the
    Python function from source on the receiving end when we deserialize the
    function, as Python is an interpreted language.

    Arguments:
    function -- either a Cascading Operation, a PyCascading-decorated Python
        function, or a native Python function
    casc_function_type -- the Cascading Operation that this Python function
        will be called by in its operate method
    """
    if isinstance(function, cascading.operation.Operation):
        return function
    if isinstance(function, DecoratedFunction):
        # Build the arguments for the constructor
        args = []
        decorators = function.decorators
        if 'numargs_expected' in decorators:
            args.append(decorators['numargs_expected'])
        if 'produces' in decorators and decorators['produces']:
            args.append(coerce_to_fields(decorators['produces']))
        # Create the appropriate type (function or filter)
        fw = casc_function_type(*args)
        function = decorators['function']
        fw.setConvertInputTuples(decorators['input_conversion'])
        if decorators['type'] in set(['map', 'reduce']):
            fw.setOutputMethod(decorators['output_method'])
            fw.setOutputType(decorators['output_type'])
            fw.setFlowProcessPassIn(decorators['flow_process_pass_in'])
        fw.setContextArgs(decorators['args'])
        fw.setContextKwArgs(decorators['kwargs'])
    else:
        # When function is a pure Python function, declared without decorators
        fw = casc_function_type()
    wrapped_func = _python_function_to_java(function)
    fw.setFunction(wrapped_func)
    return fw


class _Stackable(object):

    """An object that can be chained with '&' operations."""

    def __init__(self):
        self.stack = [self]

    def __and__(self, other):
        result = _Stackable()
        result.stack = self.stack + other.stack
        return result

    def __or__(self, other):
        p = Chainable()
        p._assembly = other._create_with_parent(self.stack)
        for s in self.stack:
            p.add_context(s.context)
        return p


class Chainable(_Stackable):

    """An object that can be chained with '|' operations."""

    def __init__(self):
        _Stackable.__init__(self)
        self._assembly = None
        self.context = set()
        self.hash = 0

    def add_context(self, ctx):
        # TODO: see if context is indeed needed
        """
        This is used to keep track of the sources connected to this pipeline
        so that a possible cache can remove them for Cascading.
        """
        # Cannot use extend because of the strings
        self.context.update(ctx)

    def get_assembly(self):
        """Return the Cascading Pipe instance that this object represents."""
        if self._assembly == None:
            self._assembly = self._create_without_parent()
        return self._assembly

    def __or__(self, other):
        result = Chainable()
        if isinstance(other, cascading.operation.Aggregator):
            other = Every(aggregator=other)
        elif isinstance(other, cascading.operation.Function):
            other = Apply(other)
        elif isinstance(other, cascading.operation.Filter):
            other = Apply(other)
        if isinstance(other, Chainable):
            result._assembly = other._create_with_parent(self)
            result.add_context(self.context)
            result.hash = self.hash ^ hash(result._assembly)
        return result

    def _create_without_parent(self):
        """Called when the Chainable is the first member of a chain.

        We want to initialize the chain with this operation as the first
        member.
        """
        raise Exception('Cannot create without parent')

    def _create_with_parent(self, parent):
        """Called when the Chainable is NOT the first member of a chain.

        Takes a PyCascading Pipe object, or a list thereof, and returns a
        corresponding Cascading Pipe instance.

        Arguments:
        parent -- the PyCascading pipe that we need to append this operation to
        """
        raise Exception('Cannot create with parent')


class Pipe(Chainable):

    """The basic PyCascading Pipe object.

    This represents an operation on the tuple stream. A Pipe object can has an
    upstream parent (unless it is a source), and a downstream child (unless it
    is a sink).
    """

    def __init__(self, name=None, *args):
        Chainable.__init__(self)
        if name:
            self.__name = name
        else:
            self.__name = 'unnamed'

    def _create_without_parent(self):
        """
        Create the Cascading operation when this is the first element of a
        chain.
        """
        return cascading.pipe.Pipe(self.__name)

    def _create_with_parent(self, parent):
        """
        Create the Cascading operation when this is not the first element
        of a chain.
        """
        return cascading.pipe.Pipe(self.__name, parent.get_assembly())


class Operation(Chainable):

    """A common base class for all operations (Functions, Filters, etc.).

    It doesn't do anything just provides the class.
    """

    def __init__(self):
        Chainable.__init__(self)


class DecoratedFunction(Operation):

    """Decorates Python functions with arbitrary attributes.

    Additional attributes and the original functions are stored in a dict
    self.decorators.
    """

    def __init__(self):
        Operation.__init__(self)
        self.decorators = {}

    def __call__(self, *args, **kwargs):
        """
        When we call the function we don't actually want to execute it, just
        to store the parameters passed to it so that we can distribute them
        to workers as a shared context.
        """
        args, kwargs = self._wrap_argument_functions(args, kwargs)
        if args:
            self.decorators['args'] = args
        if kwargs:
            self.decorators['kwargs'] = kwargs
        return self

    def _create_with_parent(self, parent):
        """
        Use the appropriate operation when the function is used in the pipe.
        """
        if self.decorators['type'] == 'map':
            return Apply(self)._create_with_parent(parent)
        elif self.decorators['type'] == 'filter':
            return Filter(self)._create_with_parent(parent)
        elif self.decorators['type'] == 'reduce':
            return Every(buffer=self)._create_with_parent(parent)
        else:
            raise Exception
        ('Function was not annotated with @map(), @filter(), or @reduce()')

    def _wrap_argument_functions(self, args, kwargs):
        """
        Just like the nested function, any arguments that are functions
        have to be wrapped.
        """
        args_out = []
        for arg in args:
            if type(arg) == types.FunctionType:
                args_out.append(_python_function_to_java(arg))
            else:
                args_out.append(arg)
        for key in kwargs:
            if type(kwargs[key]) == types.FunctionType:
                kwargs[key] = _python_function_to_java(kwargs[key])
        return (tuple(args_out), kwargs)


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
            raise Exception
        ('The number of parameters to Apply/Filter should be between 1 and 3')
        # This is the Cascading Function type
        self.__function = _wrap_function(self.__function, function_type)

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
        args = []
        args.append(pipe.get_assembly())
        if argument_selector:
            args.append(coerce_to_fields(argument_selector))
        if aggregator:
            # for now we assume it's a Cascading aggregator straight
            args.append(_wrap_function(aggregator, CascadingAggregatorWrapper))
            if output_selector:
                args.append(coerce_to_fields(output_selector))
        if assertion_level:
            args.append(assertion_level)
            args.append(assertion)
        if buffer:
            args.append(_wrap_function(buffer, CascadingBufferWrapper))
            if output_selector:
                args.append(coerce_to_fields(output_selector))
        return args

    def _create_with_parent(self, parent):
        # TODO: make output_selector=Fields.RESULTS default so that with a
        # reduce we don't have to define an Every every time if we want to do that
        if 'argument_selector' not in self.__kwargs:
            self.__kwargs['argument_selector'] = cascading.tuple.Fields.ALL
        if 'output_selector' not in self.__kwargs:
            if 'aggregator' in self.__kwargs:
                # In the case of aggregators, we want to return both the
                # groupings and the results
                self.__kwargs['output_selector'] = \
                cascading.tuple.Fields.ALL
            else:
                self.__kwargs['output_selector'] = \
                cascading.tuple.Fields.RESULTS
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
        if isinstance(parent, list):
            # We're chaining with a _Stackable object
            args = self.__create_args(pipes=parent, **self.__kwargs)
        else:
            # We're chaining with a Chainable object
            args = self.__create_args(pipe=parent, **self.__kwargs)
        return cascading.pipe.GroupBy(*args)


class CoGroup(Operation):

    """CoGroup two or more streams on common fields.

    This is a PyCascading wrapper around a Cascading CoGroup.
    """

    def __init__(self, *args, **kwargs):
        """Create a Cascading CoGroup pipe.

        Arguments:
        args[0] -- the fields on which to join

        Keyword arguments:
        group_name -- the groupName parameter for Cascading
        group_fields -- the fields on which to group
        declared_fields -- the declaredFields parameter for Cascading
        result_group_fields -- the resultGroupFields parameter for Cascading
        joiner -- the joiner parameter for Cascading
        num_self_joins -- the numSelfJoins parameter for Cascading
        lhs -- the lhs parameter for Cascading
        lhs_group_fields -- the lhsGroupFields parameter for Cascading
        rhs -- the rhs parameter for Cascading
        rhs_group_fields -- the rhsGroupFields parameter for Cascading
        """
        Operation.__init__(self)
        self.__args = args
        self.__kwargs = kwargs

    def __create_args(self,
                      group_name=None,
                      pipes=None, group_fields=None, declared_fields=None,
                      result_group_fields=None, joiner=None,
                      pipe=None, num_self_joins=None,
                      lhs=None, lhs_group_fields=None,
                      rhs=None, rhs_group_fields=None):
        # We can use an unnamed parameter only for group_fields
        if self.__args:
            group_fields = [coerce_to_fields(f) for f in self.__args[0]]
        args = []
        if group_name:
            args.append(str(group_name))
        if lhs:
            args.append(lhs.get_assembly())
            args.append(coerce_to_fields(lhs_group_fields))
            args.append(rhs.get_assembly())
            args.append(coerce_to_fields(rhs_group_fields))
            if declared_fields:
                args.append(coerce_to_fields(declared_fields))
                if result_group_fields:
                    args.append(coerce_to_fields(result_group_fields))
            if joiner:
                args.append(joiner)
        elif pipes:
            args.append([p.get_assembly() for p in pipes])
            if group_fields:
                args.append([coerce_to_fields(f) for f in group_fields])
                if declared_fields:
                    args.append(coerce_to_fields(declared_fields))
                    if result_group_fields:
                        args.append(coerce_to_fields(result_group_fields))
                else:
                    args.append(None)
                if joiner is None:
                    joiner = cascading.pipe.cogroup.InnerJoin()
                args.append(joiner)
        elif pipe:
            args.append(pipe.get_assembly())
            args.append(coerce_to_fields(group_fields))
            args.append(int(num_self_joins))
            if declared_fields:
                args.append(coerce_to_fields(declared_fields))
                if result_group_fields:
                    args.append(coerce_to_fields(result_group_fields))
            if joiner:
                args.append(joiner)
        return args

    def _create_with_parent(self, parent):
        if isinstance(parent, list):
            args = self.__create_args(pipes=parent, **self.__kwargs)
        else:
            args = self.__create_args(pipe=parent, **self.__kwargs)
        return cascading.pipe.CoGroup(*args)


def Join(*args, **kwargs):
    """Shortcut for an inner join."""
    kwargs['joiner'] = cascading.pipe.cogroup.InnerJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def OuterJoin(*args, **kwargs):
    """Shortcut for an outer join."""
    kwargs['joiner'] = cascading.pipe.cogroup.OuterJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def LeftOuterJoin(*args, **kwargs):
    """Shortcut for a left outer join."""
    # The documentation says a Cascading RightJoin is a right inner join, but
    # that's not true, it's really an outer join as it should be.
    kwargs['joiner'] = cascading.pipe.cogroup.LeftJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def RightOuterJoin(*args, **kwargs):
    """Shortcut for a right outer join."""
    kwargs['joiner'] = cascading.pipe.cogroup.RightJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


class SubAssembly(Operation):

    """Pipe for a Cascading SubAssembly.

    We can use it in PyCascading to make use of existing subassemblies,
    such as Unique.
    """

    def __init__(self, sub_assembly_class, *args):
        """Create a pipe for a Cascading SubAssembly.

        This makes use of a cascading.pipe.SubAssembly class.

        Arguments:
        sub_assembly_class -- the Cascading SubAssembly class
        *args -- parameters passed on to the subassembly's constructor when
            it's initialized
        """
        self.__sub_assembly_class = sub_assembly_class
        self.__args = args

    def _create_with_parent(self, parent):
        pipe = self.__sub_assembly_class(parent.get_assembly(), *self.__args)
        tails = pipe.getTails()
        if len(tails) == 1:
            result = tails[0]
        else:
            result = _Stackable()
            result.stack = tails
        return result
