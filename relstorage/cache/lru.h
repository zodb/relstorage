/*****************************************************************************

  Copyright (c) 2016 Zope Foundation and Contributors.
  All Rights Reserved.

  This software is subject to the provisions of the Zope Public License,
  Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
  THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
  WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
  FOR A PARTICULAR PURPOSE

 ****************************************************************************/

#include <stdint.h>
#include "ring.h"
#define __RING_H // It can't do it because its read directly by CFFI

typedef struct RSLRUEntry_struct {
	CPersistentRing ring_entry;
	uint_fast64_t frequency;
	uint_fast64_t len;
} RSLRUEntry_t;
