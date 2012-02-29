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

"""Example showing how to use caches.

A cache saves the result of an operation to a temporary folder, and running
the same script again will take the data from the cached files, instead of
executing the original pipe again.

This is useful if we want to repeatedly run the script with modifications
to parts that do not change the cached results. 
"""

import sys
from pycascading.helpers import *


@map(produces=['line'])
def find_lines_with_beginning(tuple, first_char):
    try:
        if tuple.get(1)[0] == first_char:
            return [tuple.get(1)]
    except:
        pass


@reduce(produces=['result'])
def concat_all(group, tuples, separator):
    out = ''
    for tuple in tuples:
        try:
            out = out + tuple.get(0) + separator
        except:
            pass
    return [out]


def main():
    if len(sys.argv) < 2:
        print 'A character must be given as a separator character.'
        return

    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')

    # Select the lines beginning with 'A', and save this intermediate result
    # in the cache so that we can call the script several times with
    # different separator characters
    p = flow.cache('line_begins') | (input | find_lines_with_beginning('A'))
    p | GroupBy(Fields.VALUES) | concat_all(sys.argv[1]) | output

    flow.run()
