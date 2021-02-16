====================
 Readme For Include
====================

This is a partial copy of boost 1.71.0.

The "intrusive" directory was copied in its entirety, even though, at
this writing, we only use part of it.

The remaining portions are included because intrusive uses them. 'gcc
-H' can be used to generate a header include tree in order to shake
out unnecessary parts of these includes::

    gcc -H src/relstorage/cache/c_cache.cpp -I include/ -I path/to/python -o /tmp/foo.o -c 2>&1 | grep boost


Remember to support multiple platforms, specifically POSIX and Win32.
Adding ``-DWIN32 -D_MSC_VER`` when using gcc/clang helps, but doesn't
catch everything. In particular, it misses
``boost/config/pragma_message.hpp`` and
``boost/config/stdlib/dinkumware.hpp`` (yes, apparently that's the
stdlib that `is shipped with Visual Studio <https://devblogs.microsoft.com/cppblog/c1114-stl-features-fixes-and-breaking-changes-in-vs-2013/>`_).

1.75
====

In Feb 2020 this was updated to boost 1.75 in preparation for using
boost::interprocess.

A one liner to update from a boost distribution unpacked beside this
boost/ directory::

    for i in  `find . -type f`; do gcp -f ../boost_1_75_0/boost/${i:2} $i; done
