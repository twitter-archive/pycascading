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
# Extracts a jar file and adds its contents to the PyCascading jar build.
#
# We need to extract the jar's contents as we expect that it may contain
# further jars, which would not be picked up if we didn't extract the
# whole jar.
#

usage()
{
    cat << EOF
Usage: $0 <jar1> [<jar2> ...]

Adds the jar files to the main PyCascading jar. This is useful if we have our
own or third party libraries that the PyCascading scripts use, and want to
distribute these to the Hadoop server together with the PyCascading master jar.

The jar files can contain Java classes, further jars, and Python libraries.
The Java classes should be in folders corresponding to their namespaces, as
usual for jar files. The other Java library jars must be in a \'lib\' folder in
the jar, and the Python imports must be in a \'python\' folder.

The MANIFEST file, if present, will be discarded.

Obviously, this script must be run after every new build of PyCascading for all
the jars that should be added to the PyCascading build.

EOF
}

if [ $# -eq 0 ]; then
    usage
    exit
fi

home_dir=$(pwd)
pycascading_dir=$(dirname "$0")

for j in "$@"; do
    temp=$(mktemp -d /tmp/PyCascading-tmp-XXXXXX)
    cat "$j" | (cd "$temp"; jar x)
    rm -rf "$temp/META-INF/MANIFEST.MF" 2>/dev/null
    jar -uf "$pycascading_dir/build/pycascading.jar" -C "$temp" .
    rm -rf "$temp"
done
