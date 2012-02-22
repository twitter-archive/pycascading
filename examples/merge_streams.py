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

"""Merge two streams together.

We are using Cascading GroupBy with multiple input streams to join them into
one. The streams have to have the same field names and types.

If the column names are different, Cascading won't even build the flow,
however if the column types differ, the flow is run but most likely will fail
due to different types not being comparable when grouping.
"""

from pycascading.helpers import *


def main():
    flow = Flow()
    stream1 = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/lhs.txt'))
    stream2 = flow.source(Hfs(TextDelimited(Fields(['col1', 'col2']), ' ',
                                        [Integer, String]),
                          'pycascading_data/rhs.txt'))
    output = flow.tsv_sink('pycascading_data/out')

    (stream1 & stream2) | GroupBy() | output

    flow.run()
