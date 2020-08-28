#!/opt/local/bin/bash
#
# Quick hack script to build a single release in a virtual env. Takes one
# argument, the path to python to use.
# Has hardcoded paths, probably only works on my (JAM) machine.

set -e
export WORKON_HOME=$HOME/Projects/VirtualEnvs
export VIRTUALENVWRAPPER_LOG_DIR=~/.virtualenvs
source `which virtualenvwrapper.sh`

# Make sure we use a particular known set of flags (in particular
# avoiding -march=native, https://github.com/gevent/gevent/issues/791)
# and avoiding ccache or gcc that might have come from MacPorts (in
# case of bad slopiness settings in the first and in case of ABI
# issues in the second).
export CFLAGS="-Ofast -pipe -flto -ffunction-sections --stdlib=libc++"
export CXXFLAGS="$CFLAGS -std=gnu++11"
export LDFLAGS="-Wl,-dead_strip -Wl,-merge_zero_fill_sections -flto -Wl,-headerpad_max_install_names"
unset CPPFLAGS
export CC=/usr/bin/clang
export CXX=/usr/bin/clang++
unset LDCXXSHARED
unset LDSHARED


BASE=`pwd`/../../
BASE=`greadlink -f $BASE`


cd /tmp/RelStorage
virtualenv -p $1 `basename $1`
cd `basename $1`
echo "Made tmpenv"
echo `pwd`
source bin/activate
echo cloning $BASE
git clone $BASE RelStorage
cd ./RelStorage
pip install -U pip
pip install -U setuptools cython
pip install -U wheel
# We may need different versions of deps depending on the
# version of python; that's captured in this file.
# we still need to upgrade cython first, though
# because we can get kwargs errors otherwise
pip install -U .
python ./setup.py sdist bdist_wheel
cp dist/*whl /tmp/RelStorage/
