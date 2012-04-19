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

"""
PyCascading function decorators to be used with user-defined functions.

A user-defined function is a function that gets applied as a filter or an
Each function for each tuple, or the reduce-side function for tuples in a
grouping in an Every Cascading operation.

UDFs can emit a new set of tuples (as in a Function after an Each operation),
keep or filter out tuples (a Filter after an Each), or emit aggregate values
(an Aggregator or Buffer for a group after an Every).

We use globally or locally scoped Python functions to perform these
user-defined operations. When building the data processing pipeline, we can
simply stream data through a Python function with PyCascading if it was
decorated by one of the decorators.

* A udf_'map' function is executed for each input tuple, and returns no, one, or
several new output tuples.

* A 'udf_filter' is a boolean-valued function, which should return true if the
input tuple should be kept for the output, and false if not.

* A 'udf_buffer' is a function that is applied to groups of tuples, and is the
equivalent of a Cascading Buffer. It returns an aggregate after iterating
through the tuples in the group.

Exports the following:
udf
yields
numargs_expected
python_list_expected
python_dict_expected
collects_output
produces_python_list
produces_tuples
udf_filter
udf_map
udf_buffer
"""

__author__ = 'Gabor Szabo'

import inspect

from pycascading.pipe import DecoratedFunction
from com.twitter.pycascading import CascadingBaseOperationWrapper
from com.twitter.pycascading import CascadingRecordProducerWrapper


def _function_decorator(args, kwargs, defaults={}):
    """
    A decorator to recursively decorate a function with arbitrary attributes.
    """

    def fun_decorator(function_or_callabledict):
        if isinstance(function_or_callabledict, DecoratedFunction):
            # Another decorator is next
            dff = function_or_callabledict
        else:
            # The original function comes next
            dff = DecoratedFunction.decorate_function(function_or_callabledict)
        # Add the attributes to the decorated function
        dff.decorators.update(additional_parameters)
        return dff

    additional_parameters = dict(defaults)
    additional_parameters.update(kwargs)
    if len(args) == 1 and not kwargs and (inspect.isroutine(args[0]) or isinstance(args[0], DecoratedFunction)):
        # We used the decorator without ()s, the first argument is the
        # function. We cannot use additional parameters in this case.
        return fun_decorator(args[0])
    else:
        return fun_decorator


def udf(*args, **kwargs):
    """The function can receive tuples or groups of tuples from Cascading.

    This is the decorator to use when we have a function that we want to use
    in a Cascading job after an Each or Every.
    """
    return _function_decorator(args, kwargs)


def yields(*args, **kwargs):
    """The function is a generator that yields output tuples.

    PyCascading considers this function a generator that yields one or more
    output tuples before returning. If this decorator is not used, the way the
    function emits tuples is determined automatically at runtime the first time
    the funtion is called. The alternative to yielding values is to return
    one tuple with return.

    We can safely yield Nones or not yield anything at all; no output tuples
    will be emitted in this case.  
    """
    return _function_decorator(args, kwargs, \
    { 'output_method' : CascadingRecordProducerWrapper.OutputMethod.YIELDS })


def numargs_expected(num, *args, **kwargs):
    """The function expects a num number of fields in the input tuples.

    Arguments:
    num -- the exact number of fields that the input tuples must have
    """
    return _function_decorator(args, kwargs, { 'numargs_expected' : num })


def python_list_expected(*args, **kwargs):
    """PyCascading will pass in the input tuples as Python lists.

    There is some performance penalty as all the incoming tuples need to be
    converted to Python lists.
    """
    params = dict(kwargs)
    params.update()
    return _function_decorator(args, kwargs, { 'input_conversion' : \
    CascadingBaseOperationWrapper.ConvertInputTuples.PYTHON_LIST })


def python_dict_expected(*args, **kwargs):
    """The input tuples are converted to Python dicts for this function.

    PyCascading will convert all input tuples to a Python dict for this
    function. The keys of the dict are the Cascading field names and the values
    are the values read from the tuple.

    There is some performance penalty as all the incoming tuples need to be
    converted to Python dicts.
    """
    return _function_decorator(args, kwargs, { 'input_conversion' : \
    CascadingBaseOperationWrapper.ConvertInputTuples.PYTHON_DICT })


def collects_output(*args, **kwargs):
    """The function expects an output collector where output tuples are added.

    PyCascading will pass in a Cascading TupleEntryCollector to which the
    function can add output tuples by calling its 'add' method.

    Use this if performance is important, as no conversion takes place between
    Python objects and Cascading tuples.
    """
    return _function_decorator(args, kwargs, { 'output_method' : \
    CascadingRecordProducerWrapper.OutputMethod.COLLECTS })


def produces_python_list(*args, **kwargs):
    """The function emits Python lists as tuples.

    These will be converted by PyCascading to Cascading Tuples, so this impacts
    performance somewhat.
    """
    return _function_decorator(args, kwargs, { 'output_type' : \
    CascadingRecordProducerWrapper.OutputType.PYTHON_LIST })


def produces_tuples(*args, **kwargs):
    """The function emits native Cascading Tuples or TupleEntrys.

    No conversion takes place so this is a fast way to add tuples to the
    output.
    """
    return _function_decorator(args, kwargs, { 'output_type' : \
    CascadingRecordProducerWrapper.OutputType.TUPLE })


def udf_filter(*args, **kwargs):
    """This makes the function a filter.

    The function should return 'true' for each input tuple that should stay
    in the output stream, and 'false' if it is to be removed.

    IMPORTANT: this behavior is the opposite of what Cascading expects, but
    similar to how the Python filter works!

    Note that the same effect can be attained by a map that returns the tuple
    itself or None if it should be filtered out.
    """
    return _function_decorator(args, kwargs, { 'type' : 'filter' })


def udf_map(*args, **kwargs):
    """The function decorated with this emits output tuples for each input one.

    The function is called for all the tuples in the input stream as happens
    in a Cascading Each. The function input tuple is passed in to the function
    as the first parameter and is a native Cascading TupleEntry unless the
    python_list_expected or python_dict_expected decorators are also used.

    If collects_output is used, the 2nd parameter is a Cascading
    TupleEntryCollector to which Tuples or TupleEntrys can be added. Otherwise,
    the function may return an output tuple or yield any number of tuples if
    it is a generator.

    Whether the function yields or returns will be determined automatically if
    no decorators used that specify this, and so will be the output tuple type
    (it can be Python list or a Cascading Tuple).

    Note that the meaning of 'map' used here is closer to the Python map()
    builtin than the 'map' in MapReduce. It essentially means that each input
    tuple needs to be transformed (mapped) by a custom function.

    Arguments:
    produces -- a list of output field names
    """
    return _function_decorator(args, kwargs, { 'type' : 'map' })


def udf_buffer(*args, **kwargs):
    """The function decorated with this takes a group and emits aggregates.

    A udf_buffer function must follow a Cascading Every operation, which comes
    after a GroupBy. The function will be called for each grouping on a
    different reducer. The first parameter passed to the function is the
    value of the grouping field for this group, and the second is an iterator
    to the tuples belonging to this group.

    Note that the iterator always points to a static variable in Cascading
    that holds a copy of the current TupleEntry, thus we cannot cache this for
    subsequent operations in the function. Instead, take iterator.getTuple() or
    create a new TupleEntry by deep copying the item in the loop.

    Cascading also doesn't automatically add the group field to the output
    tuples, so we need to do it manually. In fact a Cascading Buffer is more
    powerful than an aggregator, although it can be used as one. It acts more
    like a function emitting arbitrary tuples for groups, rather than just a
    simple aggregator.

    See http://groups.google.com/group/cascading-user/browse_thread/thread/f5e5f56f6500ed53/f55fdd6bba399dcf?lnk=gst&q=scope#f55fdd6bba399dcf
    """
    return _function_decorator(args, kwargs, { 'type' : 'buffer' })


def unwrap(*args, **kwargs):
    """Unwraps the tuple into function parameters before calling the function.

    This is not implemented on the Java side yet.
    """
    return _function_decorator(args, kwargs, { 'parameters' : 'unwrap' })

def tuplein(*args, **kwargs):
    return _function_decorator(args, kwargs, { 'parameters' : 'tuple' })
