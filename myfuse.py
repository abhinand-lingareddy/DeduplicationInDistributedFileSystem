#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from errno import ENOENT
from stat import S_IFDIR, S_IFLNK

from time import time

import socket
from client import client



from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class Memory(LoggingMixIn, Operations):
    'In Memory Client'

    def __init__(self,host,port):
        self.host=host
        self.port=port
        self.fd = 0
        self.l=[]
        self.operation={}
        self.tempclient=client(host,port)
        self.client={}
        now = time()
        self.files={}
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)


    def chmod(self, path, mode):
        return 0

    def chown(self, path, uid, gid):
        pass



    def getpath(self,path):
        if path[0]=="/":
            return path[1:]
        else:
            return path


    def create(self, path, mode):
        self.fd += 1
        self.operation[self.fd] = "create"
        self.client[self.fd] = client(host, port)
        path=self.getpath(path)
        self.client[self.fd].sendmetadata(path)
        self.l.append(path)
        return self.fd

    def getattr(self, path, fh=None):
        if path == '/':
            return self.files['/']
        path = self.getpath(path)
        if path not in self.l:
            raise FuseOSError(ENOENT)
        meta=self.tempclient.getmetadata(path)
        if meta is None:
            raise FuseOSError(ENOENT)
        return  meta


    def getxattr(self, path, name, position=0):
        return ''

    def listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        self.operation[self.fd] = "read"
        self.client[self.fd] = client(host, port)
        path = self.getpath(path)
        self.client[self.fd].openforreadmeta(path)
        return self.fd

    def read(self, path, size, offset, fh):
        path = self.getpath(path)
        data=self.client[fh].s.recv(size)
        if data[-1]!='~':
            return data
        else:
            return data[:-1]

    def readdir(self, path, fh):
        l=self.tempclient.list()
        l.extend(['.', '..'])
        self.l=l
        return l


    def readlink(self, path):
        path = self.getpath(path)
        return self.data[path]

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=1024, f_blocks=409600, f_bavail=204800)

    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
        self.data[target] = source

    def truncate(self, path, length, fh=None):
        self.data[path] = self.data[path][:length]
        self.files[path]['st_size'] = length

    def unlink(self, path):
        self.files.pop(path)

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        self.client[fh].s.send(data)
        return len(data)

    def flush(self, path, fh):
        if self.operation[fh]=="create":
            self.client[fh].s.send("~")
            self.client[fh].readresponse()
        self.operation.pop(fh, None)
        self.client[fh].close()
        self.client.pop(fh,None)
        return 0


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    host = socket.gethostname()
    port = 58243



    fuse = FUSE(Memory(host, port), "/home/abhinand/test2",nothreads=True, foreground=True)