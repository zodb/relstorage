#ifndef _RS_TYPES_H
#define _RS_TYPES_H
/*****************************************************************************

  Copyright (c) 2021 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/


/** Basic types */
/* The compiler used for Python 2.7 on Windows doesn't include
   either stdint.h or cstdint.h. Nor does it understand nullptr or have
   std::shared_ptr. Sigh. */
#if defined(_MSC_VER) &&  _MSC_VER <= 1500
typedef unsigned long long uint64_t;
typedef signed long long int64_t;
typedef unsigned int uint32_t;
#define nullptr NULL
#else
#include <cstdint>
#endif
/* ssize_t is also a problem, although strangely only in Python 3.10
   on Windows. Apparently earlier versions had this define somewhere:
      c:\python37\include\pyconfig.h(167)
   We should use Py_ssize_t where we can, of course.
*/
#ifdef _MSC_VER
/* This causes problems on 32-bit builds, so lets not use ssize_t.
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
*/
#else
#include <unistd.h>
#endif

#include <memory>
#include <iostream>

typedef int64_t TID_t;
// OIDs start at zero and go up from there. We use
// a signed type though to distinguish uninitialized values:
// they'll be less than 0.
typedef int64_t OID_t;

#define UNUSED(expr) do { (void)(expr); } while (0)


extern "C" {
    #include "Python.h"
}

namespace relstorage {

    // This allocator is stateless; all instances are identical.
    template <class T>
    struct PythonAllocator : public std::allocator<T> {
        // As a reminder: the `delete` expression first executes
        // the destructors, and then it calls the static ``operator delete``
        // on the type to release the storage. That's what our dispose()
        // mimics.
        PythonAllocator(const PythonAllocator& other)
            : std::allocator<T>()
        {
            UNUSED(other);
        }

        PythonAllocator(const std::allocator<T> other)
            : std::allocator<T>(other)
        {}

	template <class U>
	PythonAllocator(const std::allocator<U>& other)
	    : std::allocator<T>(other)
	{
	}

        PythonAllocator() : std::allocator<T>() {}

        T* allocate(size_t number_objects, const void* hint=0)
        {
            UNUSED(hint);
            void* p;
            if (number_objects == 1)
                p = PyObject_Malloc(sizeof(T));
            else
                p = PyMem_Malloc(sizeof(T) * number_objects);
            return static_cast<T*>(p);
        }

        void deallocate(T* t, size_t n)
        {
            void* p = t;
            if (n == 1) {
                PyObject_Free(p);
            }
            else
                PyMem_Free(p);
        }

        // Destroy and deallocate in one step.
        void dispose(T* other)
        {
            this->destroy(other);
            this->deallocate(other, 1);
        }

        // This member is deprecated in C++17 and removed in C++20,
        // but gcc 13 requires it when compiling in C++11 mode.
        template< class U >
        struct rebind {
            typedef PythonAllocator<U> other;
        };
    };
};

#endif

// Local Variables:
// flycheck-clang-include-path: ("../../include" "/opt/local/Library/Frameworks/Python.framework/Versions/2.7/include/python2.7")
// End:
