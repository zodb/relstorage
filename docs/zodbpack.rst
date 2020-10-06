========================================================
 Packing Or Reference Checking A ZODB Storage: zodbpack
========================================================
.. highlight:: guess

RelStorage also comes with a script named ``zodbpack`` that packs any
ZODB storage that allows concurrent connections (including RelStorage
and ZEO, but not including FileStorage). Use ``zodbpack`` in ``cron``
scripts. Pass the script the name of a configuration file that lists
the storages to pack, in ZConfig format. An example configuration file::

  <relstorage>
    pack-gc true
    <mysql>
      db zodb
    </mysql>
  </relstorage>

When packing a RelStorage, please read :ref:`to-know-about-pack`.

Options for ``zodbpack``
========================

``--days`` or ``-d``
    Specifies how many days of historical data to
    keep. Defaults to 1, meaning all objects newer than 1 day are
    considered reachable. This is meaningful even for history-free
    storages, since unreferenced objects are not removed from the
    database until the specified number of days have passed.

    For RelStorage, specifying a negative number means to pack to the
    most recent committed transaction.

``--prepack``
    Instructs the storage to only run the pre-pack phase of the pack but not
    actually delete anything.  This is equivalent to specifying
    ``pack-prepack-only true`` in the storage options.

``--use-prepack-state``
    Instructs the storage to only run the deletion (packing) phase, skipping
    the pre-pack analysis phase. This is equivalent to specifying
    ``pack-skip-prepack true`` in the storage options.

``--check-refs-only``
    This RelStorage only option causes the storage to run an updated
    prepack with garbage collection and then report on any objects
    that would be kept but that reference other objects that no longer
    exist. This is limited to reporting exactly one level of broken
    references.

    After completing this, you can run it again with
    ``--use-prepack-state`` and ``--days -1`` to garbage collect
    anything that needs collecting.

    .. note::

       This is new functionality as of RelStorage releases after
       October 6, 2020. Feedback is welcome!

.. program-output:: zodbpack --help
