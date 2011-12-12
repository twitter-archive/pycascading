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

"""Example showing how to use @filters and @reduces.

A 'reduce' UDF is similar to the built-in Python reduce function. It takes a
group of tuples that have been previously grouped by GroupBy, and yields an
arbitrary number of new tuples for the group (it is most useful though to do
some aggregation on the group). The tuples are fetched using an iterator.  

The data is expected in the data/ folder if run in local mode, and in the
data/ folder in the user's HDFS home if run with Hadoop. 
"""

from pycascading.helpers import *


@filter()
def starts_with_letter(tuple, letter):
    try:
        return tuple.get(1)[0].upper() == letter
    except:
        return False


@map(produces=['word_count', 'line'])
def word_count(tuple):
    return [len(tuple.get(1).split()), tuple.get(1)]


@reduce(produces=['word_count', 'count', 'first_chars'])
def count(group, tuples):
    """Counts the number of tuples in the group, and also emits a string
    containing the first character of the 'line' column this many times."""
    c = 0
    first_char = ''
    for tuple in tuples:
        c += 1
        first_char += tuple.get('line')[0]
    yield [group.get(0), c, first_char]


def main():
    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'data/town.txt'))
    output = flow.tsv_sink('data/out')
    
    p = input | starts_with_letter('A') | word_count
    p | GroupBy('word_count') | count | output
    
    flow.run()
