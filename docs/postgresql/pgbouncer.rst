======================================
 Using Connection Pooling (pgBouncer)
======================================

.. note::

   This document captures one set of experiments with pgBouncer 1.14
   and RelStorage 3.1. pgBouncer is not a regularly tested
   configuration.

   It's written in the context of a gunicorn web application serving a
   high-level of traffic using RelStorage and PostgreSQL.

Session Pooling
===============

In the default 'session' pooling mode, RelStorage works with pgBouncer
as-is. In session pooling mode, there's a one-to-one relationship
between each RelStorage connection and a PostgreSQL connection. Idle
ZODB and RelStorage connections still occupy a PG connection. On the
surface that doesn't seem to buy very much, but it might be helpful at
the extreme end of the scale by allowing us to keep a somewhat larger
gunicorn accept value: If we reduce the ZODB connection pools beneath
what PG allows, then when new temporary connections are needed, they
could come out of the pgBouncer pool, do their thing, then go back to
the pool. That would speed up (a very little bit) using temporary
connections, and it would prevent us from ever seeing the 'no more
connections' error, because we would configure pgBouncer to queue
incoming connections until a PG backend is available.

I'm not sure it's worth it though, especially because there is also a
small speed hit to using pgBouncer, as expected. Some of the
RelStorage tests are timing tests with strict tolerances. For example,
we set a lock timeout of 0.1s, fail to get a conflicted lock, and
assert that the time taken to fail was no longer than 0.1s. With
pgBouncer in the way, that test (sometimes) fails by taking anywhere
from 0.11 to 0.14s.

Transaction Pooling
===================

The pooling mode that multiplexes connections from RelStorage to PG
(and hence could allow for fewer overall PG connections),
'transaction,' **DOES NOT WORK**.

RelStorage pretty deeply embeds the idea that it owns the connection
state and sets up prepared statements, temporary tables and triggers
and assumes they persist for the duration of the connection. I was
able to hack around some of the resulting issues, but eventually I hit
a wall where some tests failed with transaction state errors whose
cause was not immediately apparent. At least once it also crashed the
Python process.

Future Implementation Ideas
---------------------------

I could imagine making RS play nicely with pgBouncer, or even pool and
share connections internally (which would benefit all databases). But
either way would take some work.

I could also imagine adding a setting to RS to make it close its
connections at the end of every transaction. That would practically
require pgBouncer to help reduce the overhead of opening new
connections for every transaction. But it should let pgBouncer work in
either 'session' or 'transaction' mode, and either mode would result
in effective multiplexing (because there would be no open idle
connections). That would also take some work, but probably somewhat
less than sharing sessions would (because of everything already in
place to gracefully deal with closed connections).
