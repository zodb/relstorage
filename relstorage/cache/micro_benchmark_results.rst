===================
 Benchmark Results
===================

Local Client Tests
==================

These tests are reported using a data size of 142MB and a cache size
of 100MB, no cache compression, a hash seed of 0 (where supported) and -O. Unless otherwise
noted, they're with Python 3.4.5.

Before any modifications (1b3910195c2b7ce666e4bd2cbecf28a79aa094b3/master)
I get these results::

  epop average 2.9002058423357084 stddev 0.04144877721013096
  mix  average 4.471249947004253 stddev 0.04006648891850737
  pop  average 3.7491709959964887 stddev 0.23962025010454424
  read average 0.6041574839910027 stddev 0.009453648365701957

With the Segmented LRU code (d030cd0984190606c2242716136c7aaaf8688fa8)
and the same settings, I get these results::

  epop average 2.4199622353335144 stddev 0.042997719276749106
  mix  average 2.5886075626670695 stddev 0.1294457218186933
  pop  average 2.1744621043326333 stddev 0.3027539534906522
  read average 0.6012111723296888 stddev 0.008994969145240476

Here's PyPy for the Segmented LRU (it's back to missing all keys for
22 groups for some reason)::

  epop average 0.688376029332 stddev 0.0117758786196
  mix  average 0.453514734904 stddev 0.0258164430069
  pop  average 0.397752523422 stddev 0.0734281091208
  read average 0.110461314519 stddev 0.0189612338017

Here's Python 2.7::

  epop average 2.29191493988 stddev 0.0770279233731
  mix  average 2.80130529404 stddev 0.331941143797
  pop  average 1.97871669134 stddev 0.0841810847293
  read average 0.489647944768 stddev 0.00126759078684

For what it's worth, it's also possible to design a trivial workload
function that generates a hit rate of 0.0 in the old code, and 100% in
the new code.

Persistence Tests
=================

Benchmark for the general approach:

Pickle is about 3x faster than marshal if we write single large
objects, surprisingly. If we stick to writing smaller objects, the
difference narrows but is still perceptible. (Naturally on PyPy the
reverse is true: pickle and zodbpickle are very slow, but marshal is
much faster. We could, but don't, take advantage of that.)

Writing 525MB of data, 655K keys (no compression):

- code as-of commit e58126a (the previous major optimizations for version 1 format)
  version 1 format, solid dict under 3.4: write: 3.8s/read 7.09s
  2.68s to update ring, 2.6s to read pickle
- in a btree under 3.4: write: 4.8s/read 8.2s
  written as single list of the items
  3.1s to load the pickle, 2.6s to update the ring
- in a dict under 3.4: write: 3.7s/read 7.6s
  written as the dict and updated into the dict
  2.7s loading the pickle, 2.9s to update the dict
- in a dict under 3.4: write: 3.0s/read 12.8s
  written by iterating the ring and writing one key/value pair
  at a time, so this is the only solution that
  automatically preserves the LRU property (and would be amenable to
  capping read based on time, and written file size); this format also lets us avoid the
  full write buffer for HIGHEST_PROTOCOL < 4
  2.5s spent in pickle.load, 8.9s spent in __setitem__,5.7s in ring.add
- in a dict: write 3.2/read 9.1s
  same as above, but custom code to set the items
  1.9s in pickle.load, 4.3s in ring.add
- same as above, but in a btree: write 2.76s/read 10.6
  1.8s in pickle.load, 3.8s in ring.add,

For the final version with optimizations (file format two), the write
time is 2.3s/read is 6.4s.

Segmented LRU
-------------

With the code before any modifications
(1b3910195c2b7ce666e4bd2cbecf28a79aa094b3) and using the same
benchmark framework along with PYTHONHASHSEED=0, we load and store
650987 objects. The write time is 1.7s and the read time is 2.4s; the
total benchmark results (number=4, repeat_count=3) are::

  read  average 8.927879446661487 stddev 0.03242392820916275
  write average 5.86237387000195 stddev 0.025450127071328835

With the code fully implementing segmented LRU
(cb663604a969ad894c74d6fc06fa47fd3be49f94), PYTHONHASHSEED=0, number=4
repeat=3 the write time is 2.5s and the read time is 2.3s. Full
benchmark results::

  read  average 6.9280044683255255 stddev 0.07165229299434527
  write average 7.996576041332446 stddev 0.05586695632417015

.. note:: In this version, even though there are 651,065 objects for a
          total size of 524,285,508, we're only loading/storing
          521,182 of them (because we're only filling the protected
          space).

When we stop aging an write and limit simply by byte count, and start
flowing items through eden, not just the protected ring, our write
time goes back to about 2.6s. Our read time increased substantially,
so we added a bulk method in C, giving us times once again comparable::

  read  average 6.240402834334721 stddev 0.5385303523379349
  write average 7.7160701316703735 stddev 0.505427296067659

A little work on optimization of writing (limiting CFFI attribute
access) gets us to these numbers, which are either faster or very
close to the same as the original numbers::

  read  average 6.509062864002772 stddev 0.08413966528299127
  write average 6.0874157809982234 stddev 0.04251385543342157

Simulations
===========

These are the results of some simulations based on the data from
http://traces.cs.umass.edu/index.php/Storage/Storage.

WebSearch1 has 1055448 operations on 480446 keys.
WebSearch2 has 4579809 operations on 726501 keys.
WebSearch3 has 4261709 operations on 707802 keys.
Financial1 has 5334987 operations on 710908 keys.
Financial2 has 3699194 operations on 296072 keys.

============  ==========  =========  =========  ========  =========
 File         Cache Size   Hits LRU  Hits SLRU  Time LRU  Time SLRU
============  ==========  =========  =========  ========  =========
Financial1      100           0.716      0.664      40.1       37.9
Financial1      512           0.839      0.826      37.7       35.5
Financial1     1024           0.881      0.893      36.3       36.0
Financial2      100           0.851      0.847      21.3       19.4
Financial2      512           0.920      0.920      18.8       17.7
Financial2     1024           0.921      0.921      18.0       18.0
WebSearch1      100           0.007      0.023      12.1       10.5
WebSearch1      512           0.042      0.120      11.8        9.5
WebSearch1     1024           0.187      0.223      11.5        9.5
WebSearch2      100           0.007      0.029      51.5       49.3
WebSearch2      512           0.044      0.146      52.5       43.4
WebSearch2     1024           0.214      0.271      46.0       39.0
WebSearch3      100           0.007      0.029      46.4       40.0
WebSearch3      512           0.048      0.147      50.1       40.3
WebSearch3     1024           0.222      0.279      42.9       35.6
============  ==========  =========  =========  ========  =========


============  =========  ==========  ==========
 File         Limit       Mem LRU    Mem SLRU
============  =========  ==========  ==========
Financial1      100        99999247   103871759
Financial1      512       511997869   514070549
Financial1     1024      1023998982  1023998356
Financial2      100        99999189   100601745
Financial2      512       511992446   512268118
Financial2     1024       606137999   606137999
WebSearch1      100        99997049   100161316
WebSearch1      512       511995883   512160824
WebSearch1     1024      1023998683  1024165996
WebSearch2      100        99997319    99989083
WebSearch2      512       511997956   511997222
WebSearch2     1024      1023995277  1023993396
WebSearch3      100        99997030    99988784
WebSearch3      512       511998677   511998234
WebSearch3     1024      1023994322  1023994405
============  =========  ==========  ==========
