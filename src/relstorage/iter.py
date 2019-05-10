"""
Database iteration helper.

Based on:
http://code.activestate.com/recipes/137270-use-generators-for-fetching-large-db-record-sets/
"""


def fetchmany(cursor, arraysize=10000):
    """Iterate over results from a cursor.

    This is intended to minimize both the number of round trips
    and the memory usage.
    """

    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result
