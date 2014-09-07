import sys as _sys

PY3 = _sys.version_info[0] == 3

if PY3:
    import builtins as builtins

    izip = zip
    intern = _sys.intern
    basestring = (str,)
    bytes = builtins.bytes
else:
    import __builtin__ as builtins

    from itertools import izip
    intern = builtins.intern
    basestring = builtins.basestring
    bytes = str

try:
    next = builtins.next
except:
    def next(iterator):
        return iterator.next()

try:
    xrange = builtins.xrange
except:
    xrange = range

# convert strs to bytes
if isinstance('', u''.__class__):
    def b(s, encoding='latin1'):
        return s.encode(encoding)

    from struct import unpack
    def u64(v):
        """Unpack an 8-byte string into a 64-bit long integer."""
        if v.__class__ == str:
            return unpack(">Q", v.encode('latin1'))[0]

        return unpack(">Q", v)[0]

else:
    def b(s, encoding='latin1'):
        return s

    from ZODB.utils import u64


try:
    from io import BytesIO
    if PY3:  # PY2 str does not work with StringIO initial value
        from io import StringIO
    else:
        from StringIO import StringIO

except ImportError:
    from cStringIO import StringIO as BytesIO
    from StringIO import StringIO

try:
    import thread
except ImportError:
    import _thread as thread

try:
    import cPickle
except ImportError:
    import pickle as cPickle

try:
    from zope.interface.declarations import implementer
    # No-op
    def implements(*args, **kwargs):
        pass
except:
    from zope.interface.declarations import implements
    # No-op deco
    def implementer(*args, **kwargs):
        def wrapper(cls):
            return cls
        return wrapper

if hasattr({}, 'iteritems'):
    def iteritems(dct):
        return dct.iteritems()
    def iterkeys(dct):
        return dct.iterkeys()
else:
    def iteritems(dct):
        return dct.items()
    def iterkeys(dct):
        return dct.keys()
    
try:
    from base64 import encodebytes, decodebytes

    def encodestring(s):
        return encodebytes(s).decode('ascii')

    def decodestring(s):
        if isinstance(s, str):
            s = s.encode('ascii')
        return decodebytes(s)

except ImportError:
    from base64 import encodestring, decodestring
    decodebytes = decodestring

if PY3:
    def ensurebytes(str_or_bytes):
        if isinstance(str_or_bytes, bytes):
            return str_or_bytes

        return str_or_bytes.encode('ascii')
else:
    def ensurebytes(str_or_bytes):
        return str(str_or_bytes)
