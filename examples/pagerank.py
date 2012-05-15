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

"""Calculates PageRank for a given graph.

We assume that there are no dangling pages with no outgoing links.
"""

import os
from pycascading.helpers import *


def test(graph_file, d, iterations):
    """This is the Python implementation of PageRank."""
    in_links = {}
    out_degree = {}
    pagerank = {}
    file = open(graph_file)
    for line in file:
        (source, dest) = line.rstrip().split()
        try:
            in_links[dest].add(source)
        except KeyError:
            in_links[dest] = set(source)
        try:
            out_degree[source] += 1
        except KeyError:
            out_degree[source] = 1
        pagerank[source] = 1.0
        pagerank[dest] = 1.0
    file.close()
    old_pr = pagerank
    new_pr = {}
    for iteration in xrange(0, iterations):
        for node in old_pr:
            new_pr[node] = (1 - d)
            try:
                new_pr[node] += \
                d * sum([old_pr[n] / out_degree[n] for n in in_links[node]])
            except KeyError:
                pass
        tmp = old_pr
        old_pr = new_pr
        new_pr = tmp
    return old_pr


def main():
    """The PyCascading job."""
    # The damping factor
    d = 0.85
    # The number of iterations
    iterations = 5

    # The directed, unweighted graph in a space-separated file, in
    # <source_node> <destination_node> format
    graph_file = 'pycascading_data/graph.txt'

    graph_source = Hfs(TextDelimited(Fields(['from', 'to']), ' ',
                                     [String, String]), graph_file)

    out_links_file = 'pycascading_data/out/pagerank/out_links'
    pr_values_1 = 'pycascading_data/out/pagerank/iter1'
    pr_values_2 = 'pycascading_data/out/pagerank/iter2'

    # Some setup here: we'll need the ougoing degree of nodes, and we will
    # initialize the pageranks of nodes to 1.0
    flow = Flow()
    graph = flow.source(graph_source)

    # Count the number of outgoing links for every node that is a source,
    # and store it in a field called 'out_degree'
    graph | group_by('from') | native.count('out_degree') | \
    flow.binary_sink(out_links_file)

    # Initialize the pageranks of all nodes to 1.0
    # This file has fields 'node' and 'pagerank', and is stored to pr_values_1
    @udf
    def constant(tuple, c):
        """Just a field with a constant value c."""
        yield [c]
    @udf
    def both_nodes(tuple):
        """For each link returns both endpoints."""
        yield [tuple.get(0)]
        yield [tuple.get(1)]
    graph | map_replace(both_nodes, 'node') | \
    native.unique(Fields.ALL) | map_add(constant(1.0), 'pagerank') | \
    flow.binary_sink(pr_values_1)

    flow.run(num_reducers=1)

    pr_input = pr_values_1
    pr_output = pr_values_2
    for iteration in xrange(0, iterations):
        flow = Flow()

        graph = flow.source(graph_source)
        pageranks = flow.meta_source(pr_input)
        out_links = flow.meta_source(out_links_file)

        # Decorate the graph's source nodes with their pageranks and the
        # number of their outgoing links
        # We could have joined graph & out_links outside of the loop, but
        # in order to demonstrate joins with multiple streams, we do it here
        p = (graph & pageranks & (out_links | rename('from', 'from_out'))) | \
        inner_join(['from', 'node', 'from_out']) | \
        rename(['pagerank', 'out_degree'], ['from_pagerank', 'from_out_degree']) | \
        retain('from', 'from_pagerank', 'from_out_degree', 'to')

        # Distribute the sources' pageranks to their out-neighbors equally
        @udf
        def incremental_pagerank(tuple, d):
            yield [d * tuple.get('from_pagerank') / tuple.get('from_out_degree')]
        p = p | map_replace(['from', 'from_pagerank', 'from_out_degree'],
                            incremental_pagerank(d), 'incr_pagerank') | \
        rename('to', 'node') | retain('node', 'incr_pagerank')

        # Add the constant jump probability to all the pageranks that come
        # from the in-links
        p = (p & (pageranks | map_replace('pagerank', constant(1.0 - d), 'incr_pagerank'))) | group_by()
        p = p | group_by('node', 'incr_pagerank', native.sum('pagerank'))

        if iteration == iterations - 1:
            # Only store the final result in a TSV file
            p | flow.tsv_sink(pr_output)
        else:
            # Store intermediate results in a binary format for faster IO
            p | flow.binary_sink(pr_output)

        # Swap the input and output folders for the next iteration
        tmp = pr_input
        pr_input = pr_output
        pr_output = tmp

        flow.run(num_reducers=1)

    print 'Results from PyCascading:', pr_input
    os.system('cat %s/.pycascading_header %s/part*' % (pr_input, pr_input))

    print 'The test values:'
    test_pr = test(graph_file, d, iterations)
    print 'node\tpagerank'
    for n in sorted(test_pr.iterkeys()):
        print '%s\t%g' % (n, test_pr[n])
