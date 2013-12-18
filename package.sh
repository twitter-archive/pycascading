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
# This script is used to deploy a PyCascading job remotely to a server
# where Hadoop is installed. The variables below are the defaults.
#

# Additional Hadoop options to be put in the run.sh runner
hadoop_options=""


# Options over, the script begins here

usage()
{
	cat <<EOF
Usage: $(basename "$0") [options] <main_script> [additional_files]

The main_script gets executed by PyCascading. All additional_files are also
copied to the remote server and submitted together with the job to Hadoop.

Options:
   -h                Show this message.

   -z                Deploy the master archives together with the job sources.
                     This is useful when we want to create a self-contained archive
                     for the whole job for a Hadoop run.

   -f <file>         Copy file to the server together with main_script, but
                     do not bundle it up for submission. This option may be
                     repeated several times for multiple files. File names
                     cannot start with a dot. This is useful if we need some
                     files to set up the flow, but not on every mapper/reducer.

   -O <hadoop_opts>  Additional Hadoop options to be put in the running script.

EOF
}


# Returns the absolute path for the parameter. We cannot use either realpath
# or readlink, as these may not be installed on MacOS.
# Thanks to Simon Radford.
realpath()
{
    if echo "$1" | grep '^/' >/dev/null; then
        # Path is absolute
        echo "$1"
    else
        # Path is relative to the working directory
        echo "$(pwd)/$1"
    fi
}


# Remove the leading slashes from a path. This is needed when we package the
# Python sources as tar does the same, and on extraction there are no leading
# slashes.
remove_leading_slash()
{
    echo "$1" | sed 's/^\/*//'
}


# Copy the master jar over first? The -m option.
master_first=no

# Run job after submission with SSH?
run_immediately='dont_run'

# Create a package file that contains everything?
package_everything=''

declare -a files_to_copy

while getopts ":hf:O:a:" OPTION; do
	case $OPTION in
        h)  usage
            exit 1
            ;;
    	a)  package_everything="$OPTARG"
            ;;
        f)	files_to_copy=("${files_to_copy[@]}" "$OPTARG")
        	;;
        O)  hadoop_options="$OPTARG"
            ;;
	esac
done
shift $((OPTIND-1))

main_file="$1"
if [ "$main_file" == "" ]; then
	usage
	exit 3
fi

home_dir=$(realpath $(dirname "$0"))
# This is the version that works both on Linux and MacOS
tmp_dir=$(mktemp -d -t PyCascading-tmp-XXXXXX)

build_dir="$home_dir/build"
if [ -e "$build_dir/pycascading.jar" -a -e "$build_dir/pycascading.tgz" ]; then
	ln -s "$build_dir/pycascading.jar" "$build_dir/pycascading.tgz" \
	"$tmp_dir"
else
    echo 'Build the PyCascading master package first in the "java" folder with ant.'
	exit 2
fi

tar -c -z -f "$tmp_dir/sources.tgz" "$@"
if [ ${#files_to_copy} -gt 0 ]; then
    tar -c -z -f "$tmp_dir/others.tgz" "${files_to_copy[@]}"
fi

#
# Create a small script on the remote server that runs the job
#
main_file=$(remove_leading_slash "$main_file")
cat >"$tmp_dir/run.sh" <<EOF
# Run the PyCascading job
job_dir=\$(dirname "\$0")
# Lazily prepare the PyCascading master folder
if [ ! -e "\$job_dir/python" ]; then
    tar -x -z -f "\$job_dir/pycascading.tgz" -C "\$job_dir"
fi
extracted_job_dir="\$job_dir/job"
if [ ! -e "\$extracted_job_dir" ]; then
    mkdir -p "\$extracted_job_dir"
    if [ -e "\$job_dir/others.tgz" ]; then
        tar -x -z -f "\$job_dir/others.tgz" -C "\$extracted_job_dir"
        rm "\$job_dir/others.tgz"
    fi
    tar -x -z -f "\$job_dir/sources.tgz" -C "\$extracted_job_dir"
fi
cd "\$job_dir/job"
hadoop $hadoop_options jar "../pycascading.jar" \\
"../python/pycascading/bootstrap.py" hadoop .. .. "$main_file" "\$@"
EOF
chmod +x "$tmp_dir/run.sh"

if [ "$package_everything" == "" ]; then
    package_everything="-"
fi
tar -c -z -h -C "$tmp_dir" -f "$package_everything" .
