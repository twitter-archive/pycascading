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

# This is the folder on the remote server where a temporary directory is
# going to be created for the submission. $HOME is only expanded on the
# remote server.
server_deploys_dir='$HOME/pycascading/deploys'

# The folder on the remote server where the PyCascading master jar will be
# placed
server_build_dir='$HOME/pycascading/master'

# Additional SSH options (see "man ssh"; private key, etc.)
ssh_options=""


# Options over, the script begins here

usage()
{
	cat << EOF
Usage: $0 [options] <main_script> [additional_files]

The main_script gets executed by PyCascading. All additional_files are also
copied to the remote server and submitted together with the job to Hadoop.

Options:
   -h                Show this message
   -m                Also deploy the PyCascading master jar before submitting
                     the job job
   -f <file>         Copy file to the server together with main_script, but
                     do not bundle it into the Hadoop jar for submission. This
                     option may be repeated several times for multiple files.
                     File names cannot start with a dot.
   -s <server>       The name of the remote server where Hadoop is installed
                     and the PyCascading jar should be deployed to
   -o <ssh_options>  Additional options for SSH (such as private key, etc.)

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


# Copy the master jar over first? The -m option.
master_first=no

declare -a files_to_copy

while getopts ":hmf:s:o:" OPTION; do
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
	esac
done
shift $((OPTIND-1))

main_file="$1"
if [ "$main_file" == "" -a $master_first == no ]; then
	usage
	exit 3
fi

home_dir=$(realpath $(dirname "$0"))
# This is the version that works both on Linux and Mac OS X
tmp_dir=$(mktemp -d -t PyCascading-tmp-XXXXXX)

if [ $master_first == yes ]; then
    master="$home_dir/build/pycascading.jar"
	if [ -a "$master" ]; then
		ln -s "$master" $home_dir/python/pycascading/bootstrap.py $tmp_dir
	else
	    echo Build the PyCascading master jar first in the \'java\' folder with ant. 
		exit 2
	fi
fi

if [ "$main_file" != "" ]; then
	mkdir $tmp_dir/sources
	mkdir $tmp_dir/other
	for i in "$@"; do
		ln -s $(realpath "$i") "$tmp_dir/sources"
	done

	for i in "${files_to_copy[@]}"; do
		ln -s $(realpath "$i") "$tmp_dir/other"
	done
fi

# Create a setup file that will be run on the deploy server
cat >"$tmp_dir/setup.sh" <<EOF
#!/usr/bin/env bash
#
# This script is run on the deploy server to set up the PyCascading job folder
#
if [ -e pycascading.jar ]; then
    # If we packaged the master jar, update it
    mkdir -p "$server_build_dir"
    mv pycascading.jar bootstrap.py "$server_build_dir"
fi
if [ -e sources ]; then
    mkdir -p "$server_deploys_dir"
    deploy_dir=\$(mktemp -d "$server_deploys_dir/XXXXXX")
    mv run.sh sources other "\$deploy_dir"
    cd "\$deploy_dir"
    if [ -e "$server_build_dir/pycascading.jar" ]; then
        cp "$server_build_dir/pycascading.jar" deploy.jar
        cp "$server_build_dir/bootstrap.py" .
        jar uf deploy.jar -C sources .
        mv other/* sources 2>/dev/null
        rm -r other
        echo "Run the job on $server with:"
        echo "   \$deploy_dir/run.sh [parameters]"
    else
        echo 'The PyCascading master jar has not yet been deployed, do a "remote_deploy.sh -m" first.'
    fi
fi
EOF
chmod +x "$tmp_dir/setup.sh"

# Create a small script on the remote server that runs the job
cat >$tmp_dir/run.sh <<EOF
#!/usr/bin/env bash
# Run the PyCascading job
cd \$(dirname "\$0")/sources
hadoop jar ../deploy.jar ../bootstrap.py hadoop "$(basename "$main_file")" "\$@"
EOF
chmod +x "$tmp_dir/run.sh"

# Upload the package to the server and run the setup script
cd "$tmp_dir"
tar czhf - . | ssh $server $ssh_options \
"dir=\$(mktemp -d -t PyCascading-tmp-XXXXXX); cd \"\$dir\"; tar xfz -; " \
"./setup.sh; rm -r \"\$dir\""
rm -r "$tmp_dir"
