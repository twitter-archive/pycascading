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

"""
Contrived example showing that you can pass functions as args to a UDF.
Also shows how to use keyword args (just the way it's expected).

Thanks to ebernhardson.
"""

from pycascading.helpers import *


def word_count_callback(value):
    return len(value.split())


@udf_map
def word_count(tuple, inc, second_inc, callback=None):
    return [inc + second_inc + callback(tuple.get(1)), tuple.get(1)]


def main():
    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))
    output = flow.tsv_sink('pycascading_data/out')

    p = input | map_replace(
        word_count(100, second_inc=200, callback=word_count_callback),
        ['word_count', 'line']) | output

    flow.run(num_reducers=1)
