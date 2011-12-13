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

"""Example showing how to pass in a 'global' context to UDFs.

The context is serialized and shipped to where the UDFs are executed. This is
a way also to perform replicated joins on constant data.

The data is expected in the pycascading_data/ folder if run in local mode,
and in the pycascading_data/ folder in the user's HDFS home if run with Hadoop. 
"""

from pycascading.helpers import *


@filter()
def starts_with_letters(tuple, letters):
    try:
        return tuple.get(1)[0].upper() in letters
    except:
        return False


def main():
    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')
    
    input | starts_with_letters(set(['A', 'T'])) | SelectFields('line') | output
    
    flow.run()
