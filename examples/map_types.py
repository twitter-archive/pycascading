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

"""Example illustrating the different types of map operations."""


from pycascading.helpers import *


def main():
    flow = Flow()
    input = flow.source(Hfs(TextLine(), 'pycascading_data/town.txt'))

    out_folder = 'pycascading_data/maps/'

    @udf(produces='word')
    def decorated_udf(tuple):
        for word in tuple.get('line').split():
            yield [word]

    def undecorated_udf(tuple):
        for word in tuple.get('line').split():
            yield [word]

    # This will create an output with one field called 'word', as the UDF
    # was declared with that
    # In this case the swap swaps out the whole input tuple with the output
    input | MapSwap(decorated_udf) | \
    flow.tsv_sink(out_folder + 'decorated_udf')

    # This will create an output with one unnamed field, but otherwise the
    # same as the previous one
    input | MapSwap(undecorated_udf) | \
    flow.tsv_sink(out_folder + 'undecorated_udf')

    # This will only replace the first ('line') field with the output of
    # the map, but 'offset' will be retained
    # Note that once we add an unnamed field, all field names will be lost
    input | MapSwap(1, undecorated_udf) | \
    flow.tsv_sink(out_folder + 'undecorated_udf_with_input_args')

    # This will create one field only, 'word', just like the first example
    input | MapSwap(undecorated_udf, 'word') | \
    flow.tsv_sink(out_folder + 'undecorated_udf_with_output_fields')

    # This one will add the new column, 'word', to all lines
    input | MapAdd(decorated_udf) | \
    flow.tsv_sink(out_folder + 'decorated_udf_all')

    input | MapAdd(1, undecorated_udf, 'word') | \
    flow.tsv_sink(out_folder + 'undecorated_udf_all')

    flow.run(num_reducers=1)
