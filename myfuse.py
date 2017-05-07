#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging

from errno import ENOENT
from stat import S_IFDIR, S_IFLNK

from time import time

import socket
from client import client



from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class RemoteFileSystem(LoggingMixIn, Operations):
    'using non persistant memory'

    def __init__(self,host,port):
        self.host=host
        self.port=port
        self.fd = 0
        self.l=[]   #readdir in memory
        self.operation={}   #map between fd and operation
        self.sharedclient=client(host, port)
        self.privateclients={} #map between fd and private clients
        now = time()
        self.rootmetadata = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
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
        self.privateclients[self.fd] = client(host, port)
        path=self.getpath(path)
        self.privateclients[self.fd].sendcreaterequest(path)
        self.l.append(path)
        return self.fd

    def getattr(self, path, fh=None):
        if path == '/':
            return self.rootmetadata
        path = self.getpath(path)
        if path not in self.l:
            raise FuseOSError(ENOENT)
        meta=self.sharedclient.metadataoperation(path)
        if meta is None:
            raise FuseOSError(ENOENT)
        return  meta


    def getxattr(self, path, name, position=0):
        return ''

    def listxattr(self, path):
        return {}

    def mkdir(self, path, mode):
        pass

    def open(self, path, flags):
        self.fd += 1
        self.operation[self.fd] = "read"
        self.privateclients[self.fd] = client(host, port)
        path = self.getpath(path)
        self.privateclients[self.fd].sendreadrequestandgetmeta(path)
        return self.fd

    def read(self, path, size, offset, fh):
        path = self.getpath(path)
        data=self.privateclients[fh].s.recv(size)
        if data[-1]!='~':
            return data
        else:
            return data[:-1]

    def readdir(self, path, fh):
        l=self.sharedclient.listoperation()
        l.extend(['.', '..'])
        self.l=l
        return l


    def readlink(self, path):
        pass

    def removexattr(self, path, name):
        pass

    def rename(self, old, new):
        pass

    def rmdir(self, path):
        pass

    def setxattr(self, path, name, value, options, position=0):
        pass

    def statfs(self, path):
        return dict(f_bsize=1024, f_blocks=409600, f_bavail=204800)

    def symlink(self, target, source):
        pass

    def truncate(self, path, length, fh=None):
        pass

    def unlink(self, path):
        pass

    def utimens(self, path, times=None):
        pass

    def write(self, path, data, offset, fh):
        self.privateclients[fh].s.send(data)
        return len(data)

    def flush(self, path, fh):
        if self.operation[fh]=="create":
            self.privateclients[fh].s.send("~")
            self.privateclients[fh].readresponse()
        self.operation.pop(fh, None)
        self.privateclients[fh].close()
        self.privateclients.pop(fh, None)
        return 0


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)
    host = socket.gethostname()
    port = 65100



    fuse = FUSE(RemoteFileSystem(host, port), "/home/abhinand/test1", nothreads=True, foreground=True)