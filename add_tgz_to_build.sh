#!/usr/bin/env bash

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

#
# Adds the packaged files to the build. It is useful if we have 3rd party
# Python libraries to be distributed together with the PyCascading master
# build.
#

usage()
{
    cat << EOF
Usage: $0 <tgz1> [<tgz2> ...]

Adds the tgz files to the main PyCascading tgz. This is useful if we have our
own or third party Python libraries that the PyCascading scripts use, and want to
distribute these to the Hadoop server together with the PyCascading master tgz.

The tgz files can contain Python libraries that will be added to the search path.

Obviously, this script must be run after every new build of PyCascading for all
the tgzs that should be added to the PyCascading build.

When distributing other Python libraries in the master, put all sources under
a "python/" folder in the tgz, since that is the folder that is picked up for
the Python search path.

EOF
}

if [ $# -eq 0 ]; then
    usage
    exit
fi

home_dir=$(pwd)
pycascading_dir=$(dirname "$0")

# BSD tar on Mac OS doesn't have the -A option, so we need to use gnutar there.
# On Linux tar should be good.
if which gnutar >/dev/null; then
    tar=gnutar
else
    tar=tar
fi

temp=$(mktemp -d -t PyCascading-tmp-XXXXXX)
gzip -d <"$pycascading_dir/build/pycascading.tgz" >"$temp/pycascading.tar"
for j in "$@"; do
    gzip -d <"$j" >"$temp/archive.tar"
    $tar -A -f "$temp/pycascading.tar" "$temp/archive.tar"
done
gzip -c <"$temp/pycascading.tar" >"$pycascading_dir/build/pycascading.tgz"
rm -rf "$temp"
