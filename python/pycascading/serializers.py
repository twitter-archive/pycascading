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

"""Serialize a Python function.

This module will serialize a Python function in one of two ways:
* if the function is globally scoped, or a method of a class, it will
  serialize it by its name, the module, and class it was defined in. Note that
  methods of nested classes cannot be serialized, as nested classes don't hold
  references to their nesting class, so they cannot be reloaded from sources.
* if the function is scoped locally (nested), we grab its source so that it
  can be reloaded on deserialization.

Exports the following:
replace_object
"""


import inspect, re, types

import pipe


def _remove_indents_from_function(code):
    """Remove leading indents from the function's source code.

    Otherwise an exec later when running the function would complain about
    the indents.
    """

    def swap_tabs_to_spaces(line):
        new_line = ''
        for i in xrange(0, len(line)):
            if line[i] == ' ':
                new_line += line[i]
            elif line[i] == '\t':
                new_line += ' ' * 8
            else:
                new_line += line[i : len(line)]
                break
        return new_line

    lines = code.split('\n')
    indent = -1
    for line in lines:
        m = re.match('^([ \t]*)def\s.*$', line)
        if m:
            #print line, 'x', m.group(1), 'x'
            indent = len(swap_tabs_to_spaces(m.group(1)))
            break
    if indent < 0:
        raise Exception('No def found for function source')
    #print 'indent', indent
    result = ''
    for line in lines:
        line = swap_tabs_to_spaces(line)
        i = 0
        while i < len(line):
            if i < indent and line[i] == ' ':
                i += 1
            else:
                break
        result += line[i : len(line)] + '\n'
    return result


def _get_source(func):
    """Return the source code for func."""
    return _remove_indents_from_function(inspect.getsource(func))


def function_scope(func):
    if (not inspect.isfunction(func)) and (not inspect.ismethod(func)):
        raise Exception('Expecting a (non-built-in) function or method')
    name = func.func_name
    module = inspect.getmodule(func)
    module_name = module.__name__
    if module_name == '__main__':
        module_name = ''
    enclosing_object = None
    if inspect.ismethod(func):
        if func.im_class == types.ClassType:
            # Function is a classmethod
            class_name = func.im_self.__name__
            if class_name in dir(module):
                # Class is a top-level class in the module
                type = 'classmethod'
                source = None
            else:
                raise Exception('Class for @classmethod is nested, and Python '
                                'cannot determine the nesting class, '
                                'thus it\'s not allowed')
        else:
            # Function is a normal method
            class_name = func.im_class.__name__
            enclosing_object = func.im_self
            if class_name in dir(module):
                # Class is a top-level class in the module
                type = 'method'
                source = None
            else:
                raise Exception('The method\'s class is not top-level')
    else:
        # The function is a global or nested function, but not a method in a class
        class_name = None
        if name in dir(module):
            # Function is a global function
            type = 'global'
            source = None
        else:
            # Function is a closure
            type = 'closure'
            source = _get_source(func)
    return (type, module_name, class_name, name, source)


def replace_object(obj):
    if inspect.isfunction(obj):
        return function_scope(obj)
    else:
        return None
