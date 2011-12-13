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

"""Simple word count example.

The data is expected in the data/ folder if run in local mode, and in the
data/ folder in the user's HDFS home if run with Hadoop. 
"""

from pycascading.helpers import *


@map(produces=['word'])
def split_words(tuple):
    for word in tuple.get(1).split():
        yield [word]

        
def main():
    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')
    
    input | split_words | GroupBy('word') | Count() | output
    
    flow.run()
