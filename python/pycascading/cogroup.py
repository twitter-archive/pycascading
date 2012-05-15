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

"""Operations related to a CoGroup pipe."""

__author__ = 'Gabor Szabo'


import cascading.pipe
import cascading.pipe.cogroup
import cascading.operation

from pycascading.pipe import Operation, coerce_to_fields, _Stackable


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
        if isinstance(parent, _Stackable):
            args = self.__create_args(pipes=parent.stack, **self.__kwargs)
        else:
            args = self.__create_args(pipe=parent, **self.__kwargs)
        return cascading.pipe.CoGroup(*args)


def inner_join(*args, **kwargs):
    """Shortcut for an inner join."""
    kwargs['joiner'] = cascading.pipe.cogroup.InnerJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def outer_join(*args, **kwargs):
    """Shortcut for an outer join."""
    kwargs['joiner'] = cascading.pipe.cogroup.OuterJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def left_outer_join(*args, **kwargs):
    """Shortcut for a left outer join."""
    # The documentation says a Cascading RightJoin is a right inner join, but
    # that's not true, it's really an outer join as it should be.
    kwargs['joiner'] = cascading.pipe.cogroup.LeftJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)


def right_outer_join(*args, **kwargs):
    """Shortcut for a right outer join."""
    kwargs['joiner'] = cascading.pipe.cogroup.RightJoin()
    if not 'declared_fields' in kwargs:
        kwargs['declared_fields'] = None
    return CoGroup(*args, **kwargs)
