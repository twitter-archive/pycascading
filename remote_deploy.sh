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

# This is the default server where the PyCascading script will be submitted
# to Hadoop. We assume we have SSH access to this server.
server=localhost

# This is the folder on the remote server where the PyCascading modules and
# jars will be cached. $HOME is only expanded on the remote server.
# Don't call it 'pycascading', as if we run a script from $HOME, some modules
# will be attempted to be imported from this folder!
server_home_dir='$HOME/.pycascading'

# This is the folder on the remote server where a temporary directory is
# going to be created for the submission.
server_deploys_dir="$server_home_dir/deploys"

# The folder on the remote server where the PyCascading master jar will be
# placed. This must be given as an absolute path name so that the master
# files can be found from any directory.
server_build_dir="$server_home_dir/master"

# Additional SSH options (see "man ssh"; private key, etc.)
ssh_options=""

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

   -m                Also deploy the PyCascading master archives before submitting
                     the job. The master archives must be on the Hadoop server
                     before a job can be run.

   -f <file>         Copy file to the server together with main_script, but
                     do not bundle it up for submission. This option may be
                     repeated several times for multiple files. File names
                     cannot start with a dot. This is useful if we need some
                     files to set up the flow, but not on every mapper/reducer.

   -s <server>       The name of the remote server where Hadoop is installed,
                     and the PyCascading scripts should be deployed to.

   -o <ssh_options>  Additional options for SSH (such as private key, etc.).
                     ssh_options is one string enclosed by "s or 's, even if
                     there are several parameters.

   -O <hadoop_opts>  Additional Hadoop options to be put in the running script.

   -r                Run the job immediately after submission with SSH. The
                     recommended way to run a script is either using screen
                     or nohup, so that the job doesn't get interrupted if the
                     terminal connection goes down. Note that no additional
                     command line parameters can be passed in this case for
                     the job.

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

declare -a files_to_copy

while getopts ":hmf:s:o:O:r" OPTION; do
	case $OPTION in
		h)	usage
         	exit 1
         	;;
        m)	master_first=yes
        	;;
        f)	files_to_copy=("${files_to_copy[@]}" "$OPTARG")
        	;;
        s)	server="$OPTARG"
        	;;
        o)  ssh_options="$OPTARG"
            ;;
        O)  hadoop_options="$OPTARG"
            ;;
        r)  run_immediately='do_run'
            ;;
	esac
done
shift $((OPTIND-1))

main_file="$1"
if [ "$main_file" == "" -a $master_first == no ]; then
	usage
	exit 3
fi

home_dir=$(realpath $(dirname "$0"))
# This is the version that works both on Linux and MacOS
tmp_dir=$(mktemp -d -t PyCascading-tmp-XXXXXX)

if [ $master_first == yes ]; then
    build_dir="$home_dir/build"
	if [ -a "$build_dir/pycascading.jar" -a \
	-a "$build_dir/pycascading.tgz" ]; then
		ln -s "$build_dir/pycascading.jar" "$build_dir/pycascading.tgz" \
		"$home_dir/python/pycascading/bootstrap.py" "$tmp_dir"
	else
	    echo 'Build the PyCascading master package first in the "java" folder with ant.'
		exit 2
	fi
fi

if [ "$main_file" != "" ]; then
	tar -c -z -f "$tmp_dir/sources.tgz" "$@"
    if [ ${#files_to_copy} -gt 0 ]; then
        tar -c -z -f "$tmp_dir/others.tgz" "${files_to_copy[@]}"
    fi
fi

#
# Create a setup file that will be run on the deploy server after everything
# is copied over.
#
cat >"$tmp_dir/setup.sh" <<EOF
#
# This script is run on the deploy server to set up the PyCascading job folder
#
if [ -e pycascading.jar ]; then
    # If we packaged the master jar, update it
    mkdir -p "$server_build_dir"
    mv pycascading.jar pycascading.tgz bootstrap.py "$server_build_dir"
    rm -rf "$server_build_dir/python"
    tar -x -z -f "$server_build_dir/pycascading.tgz" -C "$server_build_dir"
fi
if [ -e sources.tgz ]; then
    mkdir -p "$server_deploys_dir"
    deploy_dir=\$(mktemp -d "$server_deploys_dir/XXXXXX")
    mkdir "\$deploy_dir/job"
    mv run.sh "\$deploy_dir"
    tar -x -z -f sources.tgz -C "\$deploy_dir/job"
    mv sources.tgz "\$deploy_dir"
    if [ -e others.tgz ]; then
        tar -x -z -f others.tgz -C "\$deploy_dir/job"
    fi
    if [ ! -e "$server_build_dir/pycascading.jar" ]; then
        echo 'WARNING!!!'
        echo 'The PyCascading master jar has not yet been deployed, do a "remote_deploy.sh -m" first.'
        echo
    fi
    echo "Run the job on $server with:"
    echo "   \$deploy_dir/run.sh [parameters]"
fi
if [ \$1 == 'do_run' ]; then
    \$deploy_dir/run.sh "\$@"
fi
EOF
chmod +x "$tmp_dir/setup.sh"

#
# Create a small script on the remote server that runs the job
#
main_file=$(remove_leading_slash "$main_file")
cat >"$tmp_dir/run.sh" <<EOF
# Run the PyCascading job
job_dir=\$(dirname "\$0")
pycascading_dir="$server_build_dir"
#cd "\$(dirname "\$0")/job"
hadoop $hadoop_options jar "\$pycascading_dir/pycascading.jar" \\
"\$pycascading_dir/bootstrap.py" hadoop "\$pycascading_dir" \\
-a "\$pycascading_dir/pycascading.tgz" -a "\$job_dir/sources.tgz" \\
"\$job_dir/job" "$main_file" "\$@"
EOF
chmod +x "$tmp_dir/run.sh"

# Upload the package to the server and run the setup script
cd "$tmp_dir"
tar -c -z -h -f - . | ssh $server $ssh_options \
"dir=\$(mktemp -d -t PyCascading-tmp-XXXXXX); cd \"\$dir\"; tar -x -z -f -; " \
"./setup.sh $run_immediately \"\$@\"; rm -r \"\$dir\""
rm -r "$tmp_dir"
