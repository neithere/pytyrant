# -*- coding: UTF-8; tab-width: 4; c-basic-offset-mode: 4; indent-tabs-mode: nil -*-
#
# Copyright (c) 2009, Daniel Lundin
# All rights reserved.
#

"""Tokyo Tyrant Wrapper. Configure and run Tokyo Tyrant from Python.

The main goal is to make configuration and management of multiple instances
of ttserver simpler from inside a python application.

Example:

>> from pytyrant import ttserver
>> db = TTMemHashDB()
>> tt = ttserver.TokyoTyrant(db)
>> tt.run(True)
2009-06-16T15:40:45+01:00       SYSTEM  --------- logging started [11793] --------
2009-06-16T15:40:45+01:00       SYSTEM  server configuration: host=(any) port=1978
2009-06-16T15:40:45+01:00       SYSTEM  database configuration: name=*
2009-06-16T15:40:45+01:00       SYSTEM  service started: 11793
2009-06-16T15:40:45+01:00       SYSTEM  listening started

"""

__version__ = '1.0'
__author__ = 'Daniel Lundin <daniel@unempty.com>'
__license__ = """Copyright (c) 2009, Daniel Lundin, All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 'AS IS'
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE."""

import logging
import os
import signal
import subprocess
import sys

log = logging.getLogger('tokyotyrant')

# DB File Modes
TT_WRITE = 'w'
TT_READ = 'r'
TT_CREATE = 'c'
TT_TRUNCATE = 't'
TT_NOLOCK = 'e'
TT_NONBLOCKING = 'f'
TT_FILE_MODES = (TT_WRITE, TT_READ, TT_CREATE, TT_TRUNCATE, TT_NOLOCK,
                 TT_NONBLOCKING)
# DB Opts
TT_DEFLATE = 'd'
TT_LARGE = 'l'
TT_BZIP2 = 'b'
TT_TCBS = 't'
TT_OPTS = (TT_DEFLATE, TT_LARGE, TT_BZIP2, TT_TCBS)

TT_COMMAND='ttserver'


class TTDatabase(object):
    dbpath_ext = ''
    dbopts = {}

    def __init__(self, dbpath, **opts):
        self.dbpath = dbpath
        self._assert_dbpath()
        for opt,opt_val in opts.iteritems():
            if opt == 'mode' and opt_val:
                for m in opt_val:
                    if m not in TT_FILE_MODES:
                        raise ValueError, "Invalid DB file mode '%s'" % opt_val
            elif opt == 'opts' and opt_val:
                for o in opt_val:
                    if o not in TT_OPTS:
                        raise ValueError, "Invalid DB option '%s'" % opt_val
            self.dbopts[opt] = opt_val

    def __getattr__(self, attr):
        try:
            return self.dbopts[attr]
        except KeyError:
            msg = "'%s' object has no attribute '%s'" % \
                    (self.__class__.__name__, attr)
            raise AttributeError, msg

    def _assert_dbpath(self):
        if self.dbpath.endswith('.' + self.dbpath_ext):
            return True
        msg = "Database path for %s must end with .%s (was '%s')" % (self.__class__,
                                                          self.dbpath_ext, self.dbpath)
        raise ValueError, msg

    def to_cmd(self):
        arg = self.dbpath
        opts = self.opts_to_cmd()
        if opts:
            return arg + '#' + opts
        return arg

    def opts_to_cmd(self):
        return '#'.join(['%s=%s' % (k, v) for k,v in self.dbopts.iteritems()
                                            if v is not None])

class TTMemHashDB(TTDatabase):
    """In-memory hash database"""

    def __init__(self, bnum=None, capnum=None, capsiz=None):
        """
        bnum : Number of hash buckets
        capnum : Max record limit
        capsiz : Max size limit
        """
        TTDatabase.__init__(self, '*', bnum=bnum, capnum=capnum, capsiz=capsiz)

    def _assert_dbpath(self):
        pass


class TTMemBTreeDB(TTDatabase):
    """In-memory B+ Tree database"""
    
    def __init__(self, capnum=None, capsiz=None):
        """
        capnum : Capacity limit, number of records
        capsiz : Capacity limit, memory usage
        """

        TTDatabase.__init__(self, '+', capnum=capnum, capsize=capsiz)

    def _assert_dbpath(self):
        pass


class TTHashDB(TTDatabase):
    """Hash database"""

    dbpath_ext = 'tch'

    def __init__(self, dbpath, mode=None, bnum=None, apow=None, fpow=None, opts=None,
                 rcnum=None, xmsiz=None, dfunit=None):
        """
        mode : File mode. See TT_FILE_MODES.
        bnum : Number of hash buckets. (default=2 * recordcount)
        apow : Record alignment by power of 2 (default=4 => 16)
        fpow : Free block pool by power of 2 (default=10 => 1024)
        opts : DB Options. See TT_OPTS.
        rcnum : Record cache num (default=0 => Disabled)
        xmsiz : Extra mapped memory size (default=67108864 => 64MB)
        dfunit : Auto defrag unit (default=0 => Disabled) 
        """
        TTDatabase.__init__(self, dbpath, mode=mode, bnum=bnum, apow=apow,
                               fpow=fpow, opts=opts, rcnum=rcnum, xmsiz=xmsiz, dfunit=dfunit)


class TTBTreeDB(TTDatabase):
    """B+ Tree database"""

    dbpath_ext = 'tcb'

    def __init__(self, dbpath, mode=None, lmemb=None, nmemb=None, bnum=None, apow=None,
                 fpow=None, opts=None, lcnum=None, ncnum=None, xmsiz=None, dfunit=None):
        """
        mode : File mode. See TT_FILE_MODES.
        lmemb : Members / Leaf page. (default=128)
        nmemb : Members / Non-leaf page. (default=256)
        bnum: Number of hash buckets. (default=2 * pagecount)
        apow : Record alignment by power of 2. (default=4 => 16)
        fpow : Free block pool by power of 2. (default=10 => 1024)
        opts : DB Options. See TT_OPTS.
        lcnum : Leaf node cache num. (default=1024)
        ncnum : Non-Leaf node cache num. (default=512)
        xmsiz : Extra mapped memory size. (default=0 => Disabled)
        dfunit : Auto defrag unit. (default=0 => Disabled) 
        """
        TTDatabase.__init__(self, dbpath, mode=mode, lmemb=lmemb, nmemb=nmemb,
                               bnum=bnum, apow=apow, fpow=fpow, opts=opts,
                               lcnum=lcnum, ncnum=ncnum, xmsiz=xmsiz, dfunit=dfunit)


class TTFixedDB(TTDatabase):
    """Fixed-length database"""

    dbpath_ext = 'tcf'

    def __init__(self, dbpath, mode=None, width=None, limsiz=None, ):
        """
        mode : File mode. See TT_FILE_MODES.
        """
        TTDatabase.__init__(self, dbpath, mode=mode, width=width, limsiz=limsiz)


class TTTableDB(TTDatabase):
    """Table Database"""

    dbpath_ext = 'tct'

    def __init__(self, dbpath, mode=None, bnum=None, apow=None, fpow=None, opts=None,
                 rcnum=None, lcnum=None, xmsiz=None, idx=None, dfunit=None):
        """
        mode : File mode. See TT_FILE_MODES.
        bnum: Number of hash buckets. (default=131071)
        apow : Record alignment by power of 2. (default=4 => 16)
        fpow : Free block pool by power of 2. (default=10 => 1024)
        opts : DB Options. See TT_OPTS.
        rcnum : Record cache num. (default=0 => Disabled)
        lcnum : Leaf node cache num. (default=1024)
        ncnum : Non-Leaf node cache num. (default=512)
        xmsiz : Extra mapped memory size. (default=67108864 => 64MB)
        dfunit : Auto defrag unit. (default=0 => Disabled) 
        """
        TTDatabase.__init__(self, dbpath, mode=mode, bnum=bnum, apow=apow,
                               fpow=fpow, opts=opts, rcnum=rcnum, lcnum=lcnum,
                               ncnum=ncnum, xmsiz=xmsiz, idx=idx, dfunit=dfunit)


db_ext_map = dict([(c.dbpath_ext, c) for c in (TTBTreeDB, TTHashDB,
                                               TTFixedDB, TTTableDB)])
def db_factory(dbpath, *args, **kw):
    db_cls = None
    if dbpath == '*':
        db_cls = TTMemHashDB
    elif dbpath == '+':
        db_cls = TTMemBTreeDB
    else:
        ext = dbpath.rsplit('.', 1)[1]
        db_cls = db_ext_map.get(ext, None)
    if not db_cls:
        raise ValueError, "Unable to determine database type from path %s" % dbpath
    log.debug('Instatiating database of type %s' % db_cls)
    if db_cls in (TTMemHashDB, TTMemBTreeDB):
        return db_cls(*args, **kw)
    return db_cls(dbpath, *args, **kw)



class TokyoTyrant(object):
    """Convenience wrapper around Tokyo Tyrant for easy configuration and
       process management from Python programs."""

    def __init__(self, db, hostname=None, port=1978, numthreads=8, stderr=None,
                 stdout=None, pidfile=None, pidfile_kill=False,
                 log_level=logging.WARNING, ulog_path=None, ulog_limit=None,
                 ulog_async=False, serverid=None, repl_master_host=None,
                 repl_master_port=None, repl_ts_path=None, lua_ext=None,
                 lua_cron_cmd=None, lua_cron_period=60, cmds_forbidden=None,
                 cmds_allowed=None, exec_cmd = TT_COMMAND):
        if not isinstance(db, TTDatabase):
            raise TypeError, "db must be a TTDatabase instance"
        self.db = db
        self.hostname = hostname
        self.port = port
        self.numthreads = numthreads
        self.stderr = stderr
        self.stdout = stdout
        self.pidfile = pidfile
        self.pidfile_kill = pidfile_kill
        self.log_level = log_level
        self.ulog_path = ulog_path
        self.ulog_limit = ulog_limit
        self.ulog_async = ulog_async
        self.serverid = serverid
        self.repl_master_host = repl_master_host
        self.repl_master_port = repl_master_port
        self.repl_ts_path = repl_ts_path
        self.lua_ext = lua_ext
        self.lua_cron_cmd = lua_cron_cmd
        self.lua_cron_period = lua_cron_period
        self.cmds_forbidden = cmds_forbidden
        self.cmds_allowed = cmds_allowed
        self.exec_cmd = exec_cmd

    def to_cmd(self):
        args = [self.exec_cmd]

        def _add_arg(attr, arg, flag=False):
            if attr is None:
                return
            arg = '-%s' % arg
            if flag:
                if attr:
                    args.append(arg)
            else:
                args.extend((arg, str(attr)))

        _add_arg(self.hostname, 'host')
        _add_arg(self.port, 'port')
        _add_arg(self.numthreads, 'thnum')
        _add_arg(self.pidfile, 'pid')
        _add_arg(self.pidfile_kill, 'kl', True)
        _add_arg(self.ulog_path, 'ulog')
        _add_arg(self.ulog_limit, 'ulim')
        _add_arg(self.ulog_async, 'uas', True)
        _add_arg(self.repl_master_host, 'mhost')
        _add_arg(self.repl_master_port, 'mport')
        _add_arg(self.repl_ts_path, 'rts')
        _add_arg(self.lua_ext, 'ext')

        if self.lua_cron_cmd:
            _add_arg('%s %d' % (self.lua_cron_cmd, self.lua_cron_period), 'extpc')

        if self.cmds_forbidden:
            _add_arg(','.join(self.cmds_forbidden), 'mask')

        if self.cmds_allowed:
            _add_arg(','.join(self.cmds_allowed), 'unmask')

        if self.log_level > logging.INFO:
            _add_arg(True, 'le', True)
        elif self.log_level < logging.INFO:
            _add_arg(True, 'ld', True)

        args.append(self.db.to_cmd())

        return args

    def run(self, wait=False):
        pargs = self.to_cmd()
        log.debug('Starting process: %s' % ' '.join(pargs))
        self._proc = subprocess.Popen(pargs, stdout=self.stdout, stderr=self.stderr) 
        if wait:
            self.wait()

    def run_wait(self):
        self.run(True)

    def wait(self):
        try:
            self._proc.wait()
        except KeyboardInterrupt:
            pass

    def poll(self):
        try:
            return self._proc.poll()
        except:
            return False

    def restart(self):
        os.kill(self._proc.pid, signal.SIGHUP)
        
    def stop(self):
        os.kill(self._proc.pid, signal.SIGTERM)
        self.wait()




# vim:set ft=python fileencoding=utf-8 ai ts=4 sts=4 sw=4:
