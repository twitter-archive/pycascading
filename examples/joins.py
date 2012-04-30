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

"""Example showing the joining and splitting of tuple streams."""


from pycascading.helpers import *


@udf_map(produces=['ucase_lhs2', 'rhs2'])
def upper_case(tuple):
    """Return the upper case of the 'lhs2' column, and the 'rhs2' column"""
    return [tuple.get('lhs2').upper(), tuple.get('rhs2')]


def main():
    flow = Flow()
    lhs = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/lhs.txt'))
    rhs = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/rhs.txt'))
    output1 = flow.tsv_sink('pycascading_data/out1')
    output2 = flow.tsv_sink('pycascading_data/out2')

    # Join on the first columns ('col1' for both) of lhs and rhs inputs
    # We need to use declared_fields if the field names since the field names
    # of the two pipes overlap
    p = (lhs & rhs) | inner_join(['col1', 'col1'],
                                 declared_fields=['lhs1', 'lhs2', 'rhs1', 'rhs2'])

    # Save the 2nd and 4th columns of p to output1
    p | retain('lhs2', 'rhs2') | output1

    # Join on the upper-cased first column of p and the 2nd column of rhs,
    # and save the output to output2
    ((p | upper_case) & (rhs | retain('col2'))) | \
    inner_join(['ucase_lhs2', 'col2']) | output2

    flow.run(num_reducers=2)
