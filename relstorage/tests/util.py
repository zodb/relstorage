
import time

def wait_until(label=None, func=None, timeout=30, onfail=None):
    """Copied from ZEO.tests.forker, because it does not exist in ZODB 3.8"""
    if label is None:
        if func is not None:
            label = func.__name__
    elif not isinstance(label, basestring) and func is None:
        func = label
        label = func.__name__

    if func is None:
        def wait_decorator(f):
            wait_until(label, f, timeout, onfail)

        return wait_decorator

    giveup = time.time() + timeout
    while not func():
        if time.time() > giveup:
            if onfail is None:
                raise AssertionError("Timed out waiting for: ", label)
            else:
                return onfail()
        time.sleep(0.01)


try:
    from ZEO.ClientStorage import BlobCacheLayout
except ImportError:
    # ZODB 3.8.  The blob directory must be shared.
    shared_blob_dir_choices = (True,)
    support_blob_cache = False
else:
    # ZODB >= 3.9.  The blob directory can be a private cache.
    shared_blob_dir_choices = (False, True)
    support_blob_cache = True
