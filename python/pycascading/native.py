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

"""Aggregators, filters, functions, and assemblies adapted to PyCascading.

These useful operations are provided by Cascading.
"""

__author__ = 'Gabor Szabo'


import cascading.operation.aggregator as aggregator
import cascading.operation.filter as filter
import cascading.operation.function as function
import cascading.pipe.assembly as assembly

from pycascading.pipe import coerce_to_fields, SubAssembly


def average(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Average(*args)


def count(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Count(*args)


def first(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.First(*args)


def last(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Last(*args)


def max(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Max(*args)


def min(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Min(*args)


def sum(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    return aggregator.Sum(*args)


def limit(lim):
    return filter.Limit(lim)


def sample(*args):
    return filter.Sample(*args)


def un_group(*args):
    args = list(args)
    if args:
        args[0] = coerce_to_fields(args[0])
    if len(args) > 1:
        if isinstance(args[1], (list, tuple)):
            new_arg = []
            for f in args[1]:
                new_arg.append(coerce_to_fields(f))
            args[1] = new_arg
        else:
            args[1] = coerce_to_fields(args[1])
    if len(args) > 2:
        if isinstance(args[2], (list, tuple)):
            new_arg = []
            for f in args[2]:
                new_arg.append(coerce_to_fields(f))
            args[2] = new_arg
    return function.UnGroup(*args)


def average_by(*args):
    args = list(args)
    if len(args) > 0:
        args[0] = coerce_to_fields(args[0])
    if len(args) > 1:
        args[1] = coerce_to_fields(args[1])
    if len(args) > 2:
        args[2] = coerce_to_fields(args[2])
    return SubAssembly(assembly.AverageBy, *args)


def count_by(*args):
    args = list(args)
    if len(args) > 0:
        args[0] = coerce_to_fields(args[0])
    if len(args) > 1:
        args[1] = coerce_to_fields(args[1])
    return SubAssembly(assembly.CountBy, *args)


def sum_by(*args):
    # SumBy has at least 3 parameters
    args = list(args)
    for i in xrange(0, 3):
        args[i] = coerce_to_fields(args[i])
    return SubAssembly(assembly.SumBy, *args)


def unique(*args):
    args = list(args)
    args[0] = coerce_to_fields(args[0])
    return SubAssembly(assembly.Unique, *args)
