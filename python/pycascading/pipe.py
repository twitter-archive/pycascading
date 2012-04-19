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

"""Build and execute Cascading flows in Python.

Flows are built from Cascading operations that reshape, join, and split
streams. Some operations make use of user-defined functions, for instance, the
Each operation applies an UDF to each tuple seen in the stream.

Exports the following:
Pipe
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


import types, inspect, pickle

import cascading.pipe
import cascading.tuple
import cascading.operation
import cascading.pipe.cogroup
from com.twitter.pycascading import CascadingBaseOperationWrapper, \
CascadingRecordProducerWrapper

import serializers

from java.io import ObjectOutputStream


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


def wrap_function(function, casc_function_type):
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
        if decorators['type'] in set(['map', 'buffer', 'auto']):
            fw.setOutputMethod(decorators['output_method'])
            fw.setOutputType(decorators['output_type'])
        fw.setContextArgs(decorators['args'])
        fw.setContextKwArgs(decorators['kwargs'])
    else:
        # When function is a pure Python function, declared without decorators
        fw = casc_function_type()
    fw.setFunction(function)
    fw.setWriteObjectCallBack(serializers.replace_object)
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
        result = Chainable()
        result._assembly = other._create_with_parent(self)
        for s in self.stack:
            result.add_context(s.context)
        return result


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
            import every
            other = every.Every(aggregator=other)
        elif isinstance(other, cascading.operation.Function):
            import each
            other = each.Apply(other)
        elif isinstance(other, cascading.operation.Filter):
            import each
            other = each.Apply(other)
        elif inspect.isroutine(other):
            other = DecoratedFunction.decorate_function(other)
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
        my_type = self.decorators['type']
        if my_type == 'auto':
            # Determine the type of function automatically based on the parent
            if isinstance(parent, Chainable) and \
            isinstance(parent.get_assembly(), cascading.pipe.GroupBy):
                my_type = 'buffer'
            else:
                raise Exception('Function was not decorated, and I cannot ' \
                                'decide if it is a map or a filter')
        if my_type == 'map':
            import each
            return each.Apply(self)._create_with_parent(parent)
        elif my_type == 'filter':
            import pycascading.each
            return pycascading.each.Filter(self)._create_with_parent(parent)
        elif my_type == 'buffer':
            import every
            return every.Every(buffer=self)._create_with_parent(parent)
        else:
            raise Exception('Function was not annotated with ' \
                            '@udf_map(), @udf_filter(), or @udf_buffer()')

    def _wrap_argument_functions(self, args, kwargs):
        """
        Just like the nested function, any arguments that are functions
        have to be wrapped.
        """
        args_out = []
        for arg in args:
            if type(arg) == types.FunctionType:
#                args_out.append(_python_function_to_java(arg))
                args_out.append(arg)
            else:
                args_out.append(arg)
        for key in kwargs:
            if type(kwargs[key]) == types.FunctionType:
#                kwargs[key] = _python_function_to_java(kwargs[key])
                pass
        return (tuple(args_out), kwargs)

    @classmethod
    def decorate_function(cls, function):
        """Return a DecoratedFunction with the default parameters set."""
        dff = DecoratedFunction()
        # This is the user-defined Python function
        dff.decorators['function'] = function
        # If it's used as an Each, Every, or Filter function
        dff.decorators['type'] = 'auto'
        dff.decorators['input_conversion'] = \
        CascadingBaseOperationWrapper.ConvertInputTuples.NONE
        dff.decorators['output_method'] = \
        CascadingRecordProducerWrapper.OutputMethod.YIELDS_OR_RETURNS
        dff.decorators['output_type'] = \
        CascadingRecordProducerWrapper.OutputType.AUTO
        dff.decorators['args'] = None
        dff.decorators['kwargs'] = None
        return dff


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
