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

"""Example demonstrating the use of arbitrary Python (or Java) data in tuples.

The fields have to implement Serializable.

Currently these fields cannot be joined on, since we do not want to
deserialize them for each comparison. We are also doing a join here to test
the serializers.
"""


from pycascading.helpers import *


@map(produces=['col1', 'col2', 'info'])
def add_python_data(tuple):
    """This function returns a Python data structure as well."""
    return [ tuple.get(0), tuple.get(1), [ 'first', { 'key' : 'value' } ]]


def main():
    flow = Flow()
    lhs = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/lhs.txt'))
    rhs = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/rhs.txt'))

    ((lhs | add_python_data) & rhs) | Join(['col1', 'col1'],
        declared_fields=['lhs1', 'lhs2', 'info', 'rhs1', 'rhs2']) | \
        flow.tsv_sink('pycascading_data/out')

    flow.run()
