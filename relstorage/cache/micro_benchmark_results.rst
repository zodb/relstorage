===================
 Benchmark Results
===================

Local Client Tests
==================

Before any changes:
cache_local_mb = 500, datasize = 142, comp=none
epop average 3.304353015982391 stddev 0.1057548559581552
mix  average 2.922693134013874 stddev 0.014240008454610707
pop  average 2.2137693379966854 stddev 0.09458639191519878
read average 1.0852473539998755 stddev 0.023173488388016424

cache_local_mb = 100, datasize=142, comp=default
epop average 30.283703678986058 stddev 0.349105696895158
mix  average 32.43547729967395 stddev 0.6131160273617585
pop  average 31.683537834013503 stddev 0.9313916809959417
read average 0.7965960823348723 stddev 0.013812922826548332

cache_local_mb = 100, datasize=142, comp=none
epop average 3.8289742503354014 stddev 0.138905518890246
mix  average 6.044395485989905 stddev 0.12402917755863634
pop  average 4.849317686690483 stddev 0.3407186386084065
read average 0.7788464936699407 stddev 0.011301417502572604

Following numbers are all with 100/142/none (Python 3.4),
except as noted

Tracking popularity, but not aging:
epop average 3.8351666433348632 stddev 0.016045702030828404
mix  average 6.063804395322222 stddev 0.05007505835225963
pop  average 4.915782862672738 stddev 0.20628836098923425
read average 0.8606604933350658 stddev 0.01461748647882393

Aging periodically, adjusted for size. We aged three times
during the 'mixed' workload at about  0.024s each. That should be
linear, so the 800MB case would take 0.14s....but it would only be done
every 9,091,000 operations.

I noticed differences accounted for by hash ranomization between runs.
From now on, run with 'PYTHONHASHSEED=0 python -O ...'
Still same code
epop average 3.896360943307324 stddev 0.05112068256616049
mix  average 6.08575853901372 stddev 0.0651629903238879
pop  average 4.854507520659051 stddev 0.16709270096300968
read average 0.9192146409768611 stddev 0.010830646982195127

Eden generation, unoptimized
epop average 8.394099957639506 stddev 0.1772435870640342
mix  average 5.722020475019235 stddev 0.11354930215079416
pop  average 9.779178152016053 stddev 0.2953541870308067
read average 0.9772441836539656 stddev 0.010378042791130002

Full segmented LRU, unoptimized
epop average 9.370241302996874 stddev 0.20596681998331154
mix  average 8.483468734678658 stddev 0.051873515883878445
pop  average 9.935747388653303 stddev 0.10084787068640827
read average 0.42481018069277826 stddev 0.0225555561

^ The incredibly low read times are due to *nothing* actually
getting promoted to the protected generation, so there is
nothing to read. Once we pre-populate the protected segment,
that times goes to 3.8 seconds (python 2.7):

read average 3.85280092557 stddev 0.29397446809

With that pre-population done, and compression back on, we
still find the 3s slowdown for reads, which is unacceptable:

epop average 37.56022281331631 stddev 2.3185901813555603
mix  average 32.643460757661764 stddev 0.4262010697672024
pop  average 34.260926674314156 stddev 0.5609715096223009
read average 3.956128183985129 stddev 0.20373283218514573

If we add a function for dedicated access to the LRU item,
our times almost become acceptable:
- mix is faster than original;
- pop is faster than original;
- epop is slightly slower than original;
- read is about twice as slow as the original.
epop average 5.056254096988899 stddev 0.07991501317283245
mix  average 3.79573760201068 stddev 0.17857980670336743
pop  average 2.814852144336328 stddev 0.3669570906504935
read average 1.3201618876773864 stddev 0.008809367575729931

^ Sigh, no, those times were a bug. our dedicated LRU access method
was actually returning the MRU item, which means we usually skipped all
the manipulation of lists. With that fixed, and some minor opts, we're at these times:
- mix is faster; everything else is slower; read is *much* slower.
epop average 7.55050067200015 stddev 0.1184560690858881
mix  average 5.364189800301877 stddev 0.015863679783528113
pop  average 5.598568096330079 stddev 0.31457757182924395
read average 3.0701343226634585 stddev 0.19113773305717596

PyPy is *wicked* fast at this, BTW
epop average 1.30581800143 stddev 0.0167331686883
mix  average 0.690402587255 stddev 0.0491290787176
pop  average 0.915206670761 stddev 0.126843920891
read average 0.579447031021 stddev 0.0305893529027

""Inlining"" Ring.add into Ring.move_entry_from_other_ring
got us down to about 2.5s on the read test (by not allocating new
CPersistentRing structures).
Introducing the C function ring_move_to_head_from_foreign gives us these numbers:
epop average 6.472030825718927 stddev 0.06240653685961519
mix  average 5.4434908026984585 stddev 0.07889832553078663
pop  average 4.920183609317367 stddev 0.23233663177104144
read average 1.7981388796276103 stddev 0.0625821728436513

Here are more modern numbers for the same benchmark suite. Before
modifications (1b3910195c2b7ce666e4bd2cbecf28a79aa094b3/master) with
PYTHONHASHSEED=0 and -O, I get these results:

epop average 2.9002058423357084 stddev 0.04144877721013096
mix  average 4.471249947004253 stddev 0.04006648891850737
pop  average 3.7491709959964887 stddev 0.23962025010454424
read average 0.6041574839910027 stddev 0.009453648365701957

(I can't explain why these numbers are so much better than the earlier
numbers, except maybe particularly bad hash seeds? And I think the
machine had been rebooted.)

With the Segmented LRU code (cb663604a969ad894c74d6fc06fa47fd3be49f94)
and the same settings, I get these results:

epop average 2.428182589666297 stddev 0.009184376367680394
mix  average 2.637423994000225 stddev 0.07304547795654234
pop  average 2.274654309003381 stddev 0.3010997744565152
read average 0.5964773539938809 stddev 0.009971825488847518

Here's PyPy for the Segmented LRU (it's back to missing all keys for
22 groups for some reason):

epop average 0.688376029332 stddev 0.0117758786196
mix  average 0.453514734904 stddev 0.0258164430069
pop  average 0.397752523422 stddev 0.0734281091208
read average 0.110461314519 stddev 0.0189612338017

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
total benchmark results (number=4, repeat_count=3) are:

read  average 8.927879446661487 stddev 0.03242392820916275
write average 5.86237387000195 stddev 0.025450127071328835

With the code fully implementing segmented LRU
(cb663604a969ad894c74d6fc06fa47fd3be49f94), PYTHONHASHSEED=0, number=4
repeat=3 the write time is 2.5s and the read time is 2.3s. Full
benchmark results:

read  average 6.9280044683255255 stddev 0.07165229299434527
write average 7.996576041332446 stddev 0.05586695632417015

.. note:: In this version, even though there are 651,065 objects for a
          total size of 524,285,508, we're only loading/storing
          521,182 of them (because we're only filling the protected
          space).

When we stop aging an write and limit simply by byte count, and start
flowing items through eden, not just the protected ring, our write
time goes back to about 2.6s. Our read time increased substantially,
so we added a bulk method in C, giving us times once again comparable:

read  average 6.240402834334721 stddev 0.5385303523379349
write average 7.7160701316703735 stddev 0.505427296067659

A little work on optimization of writing (limiting CFFI attribute
access) gets us to these numbers, which are either faster or very
close to the same as the original numbers:

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
WebSearch1      100           0.007      0.023      12.1       10.5
WebSearch1      512           0.042      0.120      11.8        9.5
WebSearch1     1024           0.187      0.223      11.5        9.5
WebSearch2      100           0.007      0.029      51.5       49.3
WebSearch2      512           0.044      0.146      52.5       43.4
WebSearch2     1024           0.214      0.271      46.0       39.0
WebSearch3      100           0.007      0.029      46.4       40.0
WebSearch3      512           0.048      0.147      50.1       40.3
WebSearch3     1024           0.222      0.279      42.9       35.6
Financial1      100           0.716      0.664      40.1       37.9
Financial1      512           0.839      0.826      37.7       35.5
Financial1     1024           0.881      0.893      36.3       36.0
Financial2      100           0.851      0.847      21.3       19.4
Financial2      512           0.920      0.920      18.8       17.7
Financial2     1024           0.921      0.921      18.0       18.0
============  ==========  =========  =========  ========  =========


============  =========  ==========  ==========
 File         Limit       Mem LRU    Mem SLRU
============  =========  ==========  ==========
WebSearch1      100        99997049   100161316
WebSearch1      512       511995883   512160824
WebSearch1     1024      1023998683  1024165996
WebSearch2      100        99997319    99989083
WebSearch2      512       511997956   511997222
WebSearch2     1024      1023995277  1023993396
WebSearch3      100        99997030    99988784
WebSearch3      512       511998677   511998234
WebSearch3     1024      1023994322  1023994405
Financial1      100        99999247   103871759
Financial1      512       511997869   514070549
Financial1     1024      1023998982  1023998356
Financial2      100        99999189   100601745
Financial2      512       511992446   512268118
Financial2     1024       606137999   606137999
============  =========  ==========  ==========
