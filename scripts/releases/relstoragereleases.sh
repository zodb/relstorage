#!/opt/local/bin/bash

# Quick hack script to create many gevent releases.
# Contains hardcoded paths. Probably only works on my (JAM) machine
# (OS X 10.11)

mkdir /tmp/RelStorage/


# 2.7 is a python.org build, builds a 10_9_intel wheel
./relstoragerel.sh /usr/local/bin/python2.7

# 3.5 is a python.org build, builds a 10_6_intel wheel
# But that doesn't work on current macOS tool chains:
#  clang: error: invalid deployment target for -stdlib=libc++ (requires OS X 10.7 or later)
# ./relstoragerel.sh /usr/local/bin/python3.5

# 3.6 is a python.org build, builds a 10_9_intel wheel
./relstoragerel.sh /usr/local/bin/python3.6

# 3.7 is a python.org build, builds a 10_9_intel wheel
./relstoragerel.sh /usr/local/bin/python3.7

# Likewise for 3.8
./relstoragerel.sh /usr/local/bin/python3.8

# PyPy 4.0
./relstoragerel.sh `which pypy`
