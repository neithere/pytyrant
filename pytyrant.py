"""Pure python implementation of the binary Tokyo Tyrant 1.1.10 protocol

Tokyo Cabinet <http://tokyocabinet.sourceforge.net/> is a "super hyper ultra
database manager" written and maintained by Mikio Hirabayashi and released
under the LGPL.

Tokyo Tyrant is the de facto database server for Tokyo Cabinet written and
maintained by the same author. It supports a REST HTTP protocol, memcached,
and its own simple binary protocol. This library implements the full binary
protocol for the Tokyo Tyrant 1.1.10 in pure Python as defined here::

    http://tokyocabinet.sourceforge.net/tyrantdoc/

Typical usage is with the PyTyrant class which provides a dict-like wrapper
for the raw Tyrant protocol::

    >>> import pytyrant
    >>> t = pytyrant.PyTyrant.open('127.0.0.1', 1978)
    >>> t['__test_key__'] = 'foo'
    >>> t.concat('__test_key__', 'bar')
    >>> print t['__test_key__']
    foobar
    >>> del t['__test_key__']

"""
import socket
import struct
import UserDict

__version__ = '1.1.10'

__all__ = [
    'Tyrant', 'TyrantError', 'PyTyrant',
    'RDBMONOULOG', 'RDBXOLCKREC', 'RDBXOLCKGLB',
]

class TyrantError(Exception):
    pass

DEFAULT_PORT = 1978
MAGIC = 0xc8

RDBMONOULOG = 1 << 0
RDBXOLCKREC = 1 << 0
RDBXOLCKGLB = 1 << 1

class C:
    """
    Tyrant Protocol constants
    """
    put = 0x10
    putkeep = 0x11
    putcat = 0x12
    putshl = 0x13
    putnr = 0x18
    out = 0x20
    get = 0x30
    mget = 0x31
    vsiz = 0x38
    iterinit = 0x50
    iternext = 0x51
    fwmkeys = 0x58
    addint = 0x60
    adddouble = 0x61
    ext = 0x68
    sync = 0x70
    vanish = 0x71
    copy = 0x72
    restore = 0x73
    setmst = 0x78
    rnum = 0x80
    size = 0x81
    stat = 0x88
    misc = 0x90

def _t0(code):
    return [chr(MAGIC) + chr(code)]

def _t1(code, key):
    return [
        struct.pack('>BBI', MAGIC, code, len(key)),
        key,
    ]

def _t1FN(code, func, opts, args):
    outlst = [
        struct.pack('>BBIII', MAGIC, code, len(func), opts, len(args)),
        func,
    ]
    for k in args:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst


def _t1R(code, key, msec):
    return [
        struct.pack('>BBIQ', MAGIC, code, len(key), msec),
        key,
    ]

def _t1M(code, key, count):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), count),
        key,
    ]

def _tN(code, klst):
    outlst = [struct.pack('>BBI', MAGIC, code, len(klst))]
    for k in klst:
        outlst.extend([struct.pack('>I', len(k)), k])
    return outlst
    
def _t2(code, key, value):
    return [
        struct.pack('>BBII', MAGIC, code, len(key), len(value)),
        key,
        value,
    ]

def _t2W(code, key, value, width):
    return [
        struct.pack('>BBIII', MAGIC, code, len(key), len(value), width),
        key,
        value,
    ]

def _t3F(code, func, opts, key, value):
    return [
        struct.pack('>BBIIII', MAGIC, code, len(func), opts, len(key), len(value)),
        func,
        key,
        value,
    ]

def socksend(sock, lst):
    for chunk in lst:
        sock.send(chunk)

def socksuccess(sock):
    fail_code = ord(sock.recv(1))
    if fail_code:
        raise TyrantError(fail_code)

def socklen(sock):
    return struct.unpack('>I', sock.recv(4))[0]

def socklong(sock):
    return struct.unpack('>Q', sock.recv(8))[0]

def sockstr(sock):
    return sock.recv(socklen(sock))

def sockstrpair(sock):
    klen = socklen(sock)
    vlen = socklen(sock)
    k = sock.recv(klen)
    v = sock.recv(vlen)
    return k, v

class PyTyrant(UserDict.DictMixin):
    """
    Dict-like proxy for a Tyrant instance
    """
    @classmethod
    def open(cls, *args, **kw):
        return cls(Tyrant.open(*args, **kw))

    def __init__(self, t):
        self.t = t
    
    def has_key(self, key):
        return key in self

    def __contains__(self, key):
        try:
            self.t.vsiz(key)
        except TyrantError:
            return False
        else:
            return True
    
    def setdefault(self, key, value):
        try:
            self.t.putkeep(key, value)
        except TyrantError:
            return self[key]
        return value
    
    def __setitem__(self, key, value):
        self.t.put(key, value)

    def __getitem__(self, key):
        try:
            return self.t.get(key)
        except TyrantError:
            raise KeyError(key)
    
    def __delitem__(self, key):
        try:
            self.t.out(key)
        except TyrantError:
            raise KeyError(key)
    
    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        self.t.iterinit()
        try:
            while True:
                yield self.t.iternext()
        except TyrantError:
            pass
    
    def keys(self):
        return list(self.iterkeys())
    
    def __len__(self):
        return self.t.rnum()
    
    def clear(self):
        self.t.vanish()

    def update(self, other=None, **kwargs):
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, 'iteritems'):
            self.multi_set(other.iteritems())
        elif hasattr(other, 'keys'):
            self.multi_set([(k, other[k]) for k in other.keys()])
        else:
            self.multi_set(other)
        if kwargs:
            self.update(kwargs)

    def multi_del(self, keys, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        self.t.misc("outlist", opts, keys)
        
    def multi_get(self, keys, no_update_log=False):
        # Note that the values will become de-interleaved if a key does not exist, so don't do that.
        opts = (no_update_log and RDBMONOULOG or 0)
        if not isinstance(keys, (list, tuple)):
            keys = list(keys)
        return self.t.misc("getlist", opts, keys)
        
    def multi_set(self, items, no_update_log=False):
        opts = (no_update_log and RDBMONOULOG or 0)
        lst = []
        for k, v in items:
            lst.extend((k, v))
        self.t.misc("putlist", opts, lst)
    
    def call_func(self, func, key, value, record_locking=False, global_locking=False):
        opts = (
            (record_locking and RDBXOLCKREC or 0) |
            (global_locking and RDBXOLCKGLB or 0))
        return self.t.ext(func, opts, key, value)
    
    def get_size(self, key):
        try:
            return self.t.vsiz(key)
        except TyrantError:
            raise KeyError(key)

    def get_stats(self):
        return dict(l.split('\t', 1) for l in self.t.stat().splitlines() if l)
    
    def prefix_keys(self, prefix, maxkeys=None):
        if maxkeys is None:
            maxkeys = len(self)
        return self.t.fwmkeys(prefix, maxkeys)
    
    def concat(self, key, value, width=None):
        if width is None:
            self.t.putcat(key, value)
        else:
            self.t.putshl(key, value, width)
    
    def sync(self):
        self.t.sync()
    
    def close(self):
        self.t.close()

class Tyrant(object):
    @classmethod
    def open(cls, host='127.0.0.1', port=DEFAULT_PORT):
        sock = socket.socket()
        sock.connect((host, port))
        return cls(sock)

    def __init__(self, sock):
        self.sock = sock

    def close(self):
        self.sock.close()

    def put(self, key, value):
        """Unconditionally set key to value
        """
        socksend(self.sock, _t2(C.put, key, value))
        socksuccess(self.sock)

    def putkeep(self, key, value):
        """Set key to value if key does not already exist
        """
        socksend(self.sock, _t2(C.putkeep, key, value))
        socksuccess(self.sock)

    def putcat(self, key, value):
        """Append value to the existing value for key, or set key to
        value if it does not already exist
        """
        socksend(self.sock, _t2(C.putcat, key, value))
        socksuccess(self.sock)
    
    def putshl(self, key, value, width):
        """Equivalent to::
        
            self.putcat(key, value)
            self.put(key, self.get(key)[-width:])
        """
        socksend(self.sock, _t2W(C.putshl, key, value, width))
        socksuccess(self.sock)

    def putnr(self, key, value):
        """Set key to value without waiting for a server response
        """
        socksend(self.sock, _t2(C.putnr, key, value))
    
    def out(self, key):
        """Remove key from server
        """
        socksend(self.sock, _t1(C.out, key))
        socksuccess(self.sock)
    
    def get(self, key):
        """Get the value of a key from the server
        """
        socksend(self.sock, _t1(C.get, key))
        socksuccess(self.sock)
        return sockstr(self.sock)
    
    def _mget(self, klst):
        socksend(self.sock, _tN(C.mget, klst))
        socksuccess(self.sock)
        numrecs = socklen(self.sock)
        for i in xrange(numrecs):
            k, v = sockstrpair(self.sock)
            yield k, v
    
    def mget(self, klst):
        """Get key,value pairs from the server for the given list of keys
        """
        return list(self._mget(klst))
    
    def vsiz(self, key):
        """Get the size of a value for key
        """
        socksend(self.sock, _t1(C.vsiz, key))
        socksuccess(self.sock)
        return socklen(self.sock)
    
    def iterinit(self):
        """Begin iteration over all keys of the database
        """
        socksend(self.sock, _t0(C.iterinit))
        socksuccess(self.sock)
    
    def iternext(self):
        """Get the next key after iterinit
        """
        socksend(self.sock, _t0(C.iternext))
        socksuccess(self.sock)
        return sockstr(self.sock)
    
    def _fwmkeys(self, prefix, maxkeys):
        socksend(self.sock, _t1M(C.fwmkeys, prefix, maxkeys))
        socksuccess(self.sock)
        numkeys = socklen(self.sock)
        for i in xrange(numkeys):
            yield sockstr(self.sock)
    
    def fwmkeys(self, prefix, maxkeys):
        """Get up to the first maxkeys starting with prefix
        """
        return list(self._fwmkeys(prefix, maxkeys))
    
    def ext(self, func, opts, key, value):
        # tcrdbext opts are RDBXOLCKREC, RDBXOLCKGLB
        """Call func(key, value) with opts
        
        opts is a bitflag that can be RDBXOLCKREC for record locking
        and/or RDBXOLCKGLB for global locking"""
        socksend(self.sock, _t3F(C.ext, func, opts, key, value))
        socksuccess(self.sock)
        return sockstr(self.sock)
    
    def sync(self):
        """Synchronize the database
        """
        socksend(self.sock, _t0(C.sync))
        socksuccess(self.sock)
    
    def vanish(self):
        """Remove all records
        """
        socksend(self.sock, _t0(C.vanish))
        socksuccess(self.sock)
    
    def copy(self, path):
        """Hot-copy the database to path
        """
        socksend(self.sock, _t1(C.copy, path))
        socksuccess(self.sock)
    
    def restore(self, path, msec):
        """Restore the database from path at timestamp (in msec)
        """
        socksend(self.sock, _t1R(C.copy, path, msec))
        socksuccess(self.sock)

    def setmst(self, host, port):
        """Set master to host:port
        """
        socksend(self.sock, _t1M(C.setmst, host, port))
        socksuccess(self.sock)
    
    def rnum(self):
        """Get the number of records in the database
        """
        socksend(self.sock, _t0(C.rnum))
        socksuccess(self.sock)
        return socklong(self.sock)
    
    def size(self):
        """Get the size of the database
        """
        socksend(self.sock, _t0(C.size))
        socksuccess(self.sock)
        return socklong(self.sock)
    
    def stat(self):
        """Get some statistics about the database
        """
        socksend(self.sock, _t0(C.stat))
        socksuccess(self.sock)
        return sockstr(self.sock)
    
    def _misc(self, func, opts, args):
        # tcrdbmisc opts are RDBMONOULOG
        socksend(self.sock, _t1FN(C.misc, func, opts, args))
        socksuccess(self.sock)
        numrecs = socklen(self.sock)
        for i in xrange(numrecs):
            yield sockstr(self.sock)
    
    def misc(self, func, opts, args):
        """All databases support "putlist", "outlist", and "getlist".
        "putlist" is to store records. It receives keys and values one after the other, and returns an empty list.
        "outlist" is to remove records. It receives keys, and returns an empty list.
        "getlist" is to retrieve records. It receives keys, and returns values.
        
        opts is a bitflag that can be RDBMONOULOG to prevent writing to the update log
        """
        return list(self._misc(func, opts, args))