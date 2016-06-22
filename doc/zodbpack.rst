==========
 zodbpack
==========
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

Options for ``zodbpack``
========================

  ``--days`` or ``-d``
    Specifies how many days of historical data to keep. Defaults to 0,
    meaning no history is kept. This is meaningful even for
    history-free storages, since unreferenced objects are not removed
    from the database until the specified number of days have passed.

  ``--prepack``
    Instructs the storage to only run the pre-pack phase of the pack but not
    actually delete anything.  This is equivalent to specifying
    ``pack-prepack-only true`` in the storage options.

  ``--use-prepack-state``
    Instructs the storage to only run the deletion (packing) phase, skipping
    the pre-pack analysis phase. This is equivalent to specifying
    ``pack-skip-prepack true`` in the storage options.

.. program-output:: zodbpack --help
