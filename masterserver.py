from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from client import client
import election
import time
import os
import dedupe
from threading import Lock
import random
import kazoo
import logging
from os import listdir
from os.path import isfile, join

import sys


class server:
    def __init__(self, host, port, storage_path, elect, meta, zk):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket = s
        self.storage_path = storage_path
        self.elect = elect
        self.meta = meta
        self.zk = zk
        self.requesttokens = set()
        self.actual_content_hops = 2
        self.dedupe_content_hops = 2
        self.lock = Lock()
        self.ds = dedupe.deduplication(dedupepath=storage_path)
        self.host = host
        self.port = port
        self.currentfiles=[]
        allfiles = [f for f in listdir(storage_path) if isfile(join(storage_path, f))]
        for cfile in allfiles:
            if cfile.endswith("._temp"):
                cfile=cfile[:len("._temp")*-1]
            self.currentfiles.append(cfile)
        kazoo.recipe.watchers.ChildrenWatch(self.zk,"/metadata", self.updatemetadata)


    #Todo check if delete a file and recreate calls this or not
    def addFiles(self,newfiles):
        for newfile in newfiles:
            print "update metadata called for file", newfile
            meta_string = zk.get("/metadata/" + newfile)
            print(str(meta_string[0]))
            self.meta[newfile]=eval(str(meta_string[0]))

    def updatemetadata(self,updatedfiles):
        print "metadata update called with files"+str(updatedfiles)
        print "current metadata"+str(self.meta.keys())
        cset = set(updatedfiles)
        s = set(file for file in self.meta)
        n = cset - s
        if len(n) > 0:
            self.addFiles(n)

    # todo
    def getname(self):
        pass

    def accept(self):
        c, addr = self.serversocket.accept()
        return c

    def getclientfrominfo(self, info):
        if info is not None:
            host = info[:info.index(',')]
            port = int(info[info.index(',') + 1:])
            return client(host, port)

    def getnextclient(self):
        nextclient = self.getclientfrominfo(self.elect.childinfo)
        if nextclient is None and not self.elect.ismaster():
            nextclient = self.getclientfrominfo(self.elect.masterinfo)
        return nextclient

    def on_child_sucess1(self, filename, threadclientsocket, isclientrequest):
        if threadclientsocket is not None:
            if not isclientrequest:
                sendlib.write_socket(threadclientsocket, "sucess2")
            else:
                self.zk.create("dedupequeue/" + filename, str(self.storage_path))
                response = self.prepare_response(200)
                sendlib.write_socket(threadclientsocket, response)

    def writetochild(self, storage_path, filename, req, childclient):
        while (True and childclient is not None):
            try:
                status = filesendlib.sendresponseandfile(filesendlib.storagepathprefix(storage_path), filename,
                                                         childclient.s, req, self.ds, False)
                if status == "terminate":
                    return "terminate", childclient
                response = sendlib.read_socket(childclient.s)
                if response != "sucess1":
                    jp = jsonParser(response)
                    hashes = jp.getValue("hashes")
                    print "requested hashes " + str(hashes)
                    for hash in hashes:
                        data = self.ds.getdatafromhash(hash)
                        sendlib.write_socket(childclient.s, data)
                break
            except socket.error:
                time.sleep(60)
                childclient = self.getnextclient()
        return "sucess1", childclient

    def handlechildwrite(self, filename, req, threadclientsocket, childclient, isclientrequest):
        storage_path = filesendlib.storagepathprefix(self.storage_path)
        response, childclient = self.writetochild(storage_path, filename, req, childclient)

        self.on_child_sucess1(filename, threadclientsocket, isclientrequest)
        while response != "terminate" and childclient is not None:
            try:
                response = sendlib.read_socket(childclient.s)
                if response == "sucess2":
                    childclient.close()
                    break
            except socket.error:
                time.sleep(60)
                childclient = self.getnextclient()
                response, childclient = self.writetochild(storage_path, filename, req, childclient)

    def addownerentry(self,filename):
        if zk.exists("owner/" + filename) is not None:
            ownerh_p = "( " + str(self.host) + " : " + str(self.port) + " : " + str(self.elect.key) + " )"
            lock = self.zk.Lock("filelockpath" + filename, self.elect.key)
            with lock:
                owners = zk.get("owner/" + filename)[0]
                owners = eval(str(owners))
                owners.append(ownerh_p)
                zk.set("owner/" + filename, str(owners))


    def create(self, filename, req, threadclientsocket, hopcount, isclientrequest):
        filesendlib.recvfile(self.storage_path, filename, threadclientsocket)
        if not isclientrequest:
            if filename.endswith("._temp"):
                actualfilename = filesendlib.actualfilename(filename)
                if self.ds.actualfileexits(actualfilename):
                    #waiting for the actual content to reach completely
                    print "wait for actual content to complete "+actualfilename
                    while(not (actualfilename in self.currentfiles)):
                        print "wait for actual content to complete "+actualfilename
                        time.sleep(1)

                    self.ds.createchunkfromactualfile(filename, actualfilename)
                    print "done waiting for "+actualfilename
                    sendlib.write_socket(threadclientsocket, "sucess1")
                else:
                    missingchunkhashes = self.ds.findmissingchunk(filename)
                    response = {}
                    response["hashes"] = missingchunkhashes
                    sendlib.write_socket(threadclientsocket, str(response))
                    for missinghash in missingchunkhashes:
                        chunk = sendlib.read_socket(threadclientsocket)
                        self.ds.createChunkFile(chunk, missinghash)
                    print "done fetching missing chunks adding owner entry"
                    #if file comes before this operation completes
                    if filename in self.currentfiles:
                        os.remove(filesendlib.storagepathprefix(storage_path) + filename)
                    else:
                        self.currentfiles.append(filename[:len("._temp")*-1])
                    print "added curren file "+str(self.currentfiles)
                    self.addownerentry(filename)
            else:
                if filename in self.currentfiles:
                    os.remove(filesendlib.storagepathprefix(storage_path)+filename)
                else:
                    self.currentfiles.append(filename)
                print "added curren file " + str(self.currentfiles)
                sendlib.write_socket(threadclientsocket, "sucess1")
                self.addownerentry(filename)
        else:
            self.updatesize(filename)
            self.currentfiles.append(filename)
            if zk.exists("owner/" + filename) is None:
                ownerh_p = "( " + str(self.host) + " : " + str(self.port) + " : " + str(self.elect.key) + " )"
                owners = []
                owners.append(ownerh_p)
                zk.create("owner/" + filename, str(owners),
                          ephemeral=False, makepath=True)

        childclient = self.getnextclient()

        if hopcount > 0 and childclient is not None:
            self.handlechildwrite(filename, req, threadclientsocket, childclient, isclientrequest)
        else:
            self.on_child_sucess1(filename, threadclientsocket, isclientrequest)

    def getownersdetails(self, filename):
        if zk.exists("owner/" + filename) is not None:
            owners = zk.get("owner/" + filename)[0]
            owners = eval(str(owners))
            return owners
        return None

    def getownerhostport(self,owners,index):
        owner=owners[index]
        info = owner.split(" ")
        host = info[1]
        port = int(info[3])
        name = info[5]
        if port == self.port and host == self.host:
            return None
        return host, port,name
    def read(self, filename, threadclientsocket):
        response = self.response_dic(200)
        meta = self.read_meta(filename)
        if self.ds.actualfileexits(filename) or self.ds.dedupefileexits(filename) :
                response["meta"] = self.meta[filename]
                filesendlib.sendresponseandfile(filesendlib.storagepathprefix(self.storage_path), filename,
                                                threadclientsocket,
                                                str(response), self.ds, True)
        else:
            owners=self.getownersdetails(filename)
            for i in range(len(owners)):
                print "owners "+str(owners)
                ownerh_p=self.getownerhostport(owners,i)
                if ownerh_p is not None:
                    if zk.exists(ownerh_p[2]) is not None:
                        s = socket.socket()
                        s.connect((ownerh_p[0],ownerh_p[1]))
                        if s is not None:
                            request = {}
                            request["file_name"] = filename
                            request["operation"] = "READ"
                            sendlib.write_socket(s, str(request))
                            resp = sendlib.read_socket(s)
                            jp = jsonParser(resp)
                            if jp.getValue("status") == 200:
                                #self.meta[filename] = jp.getValue("meta")
                                response["meta"] = self.meta[filename]
                                filesendlib.recvfile(filesendlib.storagepathprefix(self.storage_path), filename, s)
                                s.close()
                                filesendlib.sendresponseandfile(filesendlib.storagepathprefix(self.storage_path), filename,
                                                            threadclientsocket,
                                                            str(response), self.ds, True)
                                self.addownerentry(filename)
                                return
            return 404



    def list(self, threadclientsocket):
        result = self.response_dic(200)
        # if os.path.exists(storage_path):
        #     files = [file for file in os.listdir(self.storage_path)
        #              if os.path.isfile(os.path.join(self.storage_path, file))]
        #     for i in range(len(files)):
        #         if files[i].endswith("._temp"):
        #             files[i] = files[i][:len("._temp") * -1]
        #     result["files"] = files
        # else:
        #     result["files"] = []
        files = [file for file in self.meta]
        result["files"] = files
        result=str(result)
        print "list operation"+result
        print "length of list operation "+str(len(result))
        sendlib.write_socket(threadclientsocket, result)

    def response_dic(self, result):
        code = {
            200:
                "OK",
            400:
                "Bad Request",
            404:
                "Not Found",
        }

        status = result
        response = {}
        response["status"] = status
        response["message"] = code[status]

        return response

    def prepare_response(self, result):
        response = self.response_dic(result)
        return str(response)
    #todo
    def store_meta_memory(self, filename, jp):
        print "added metadata for file "+filename
        self.meta[filename] = jp.getValue("meta")

    def read_meta(self, filename):
        if filename not in self.meta:
            if self.ds.actualfileexits(filename):
                st = os.lstat(filesendlib.storagepathprefix(self.storage_path) + filename)
                filemeta = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                                    'st_gid', 'st_mode', 'st_mtime', 'st_nlink',
                                                                    'st_size',
                                                                    'st_uid'))

            elif self.ds.dedupefileexits(filename):
                st = os.lstat(filesendlib.storagepathprefix(self.storage_path) + filename + "._temp")
                filemeta = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                                    'st_gid', 'st_mode', 'st_mtime', 'st_nlink',
                                                                    'st_uid'))
                filemeta['st_size'] = self.ds.findfilelength(filename)
            else:
                return None
            self.meta[filename] = filemeta
        return self.meta[filename]

    def gethopcount(self, jp):
        if not jp.has("hopcount"):
            hopcount = self.actual_content_hops
        else:
            hopcount = jp.getValue("hopcount") - 1
        return hopcount

    def updatehopcountrequest(self, jp, hopcount):
        respdic = jp.getdic()
        respdic["hopcount"] = hopcount
        req = str(respdic)
        return req

    def updatesize(self, filename):
        storagepath = filesendlib.storagepathprefix(self.storage_path)
        if os.path.exists(storagepath + filename):
            size = os.path.getsize(storagepath + filename)
        else:
            size = self.ds.findfilelength(filename)
        self.meta[filename]["st_size"] = size
        self.meta[filename]["st_ctime"] = time.time()

    def performdedupe(self):
        while (True):
            pending_files = self.zk.get_children("dedupequeue")
            for file in pending_files:
                if file in self.currentfiles and self.ds.actualfileexits(file):
                    try:
                        if zk.exists("dedupeserver/" + file) is None:
                            zk.create("dedupeserver/" + file, str(self.storage_path), ephemeral=True, makepath=True)
                            print "deduping file " + file
                            self.ds.write(file)
                            request = client.createrequest(file + "._temp")
                            request["meta"] = self.read_meta(file)
                            request["hopcount"] = self.dedupe_content_hops
                            requestToken = random.getrandbits(128)
                            request["token"] = requestToken
                            self.requesttokens.add(requestToken)
                            self.handlechildwrite(file + "._temp", str(request), None, self.getnextclient(), False)
                            zk.delete("dedupequeue/" + file)
                            owners = self.getownersdetails(file)
                            for i in range(len(owners)):
                                ownerh_p = self.getownerhostport(owners, i)
                                if ownerh_p is not None:
                                    if zk.exists(ownerh_p[2]) is not None:
                                            #writing to first existing owner
                                            c=client(ownerh_p[0],ownerh_p[1])
                                            self.handlechildwrite(file + "._temp", str(request), None,c, False)
                                            c.close()
                                            break
                                # if ownerh_p is None, then ownerh_p belongs to the same node, so terminating
                                else:
                                    break
                    except NodeExistsError:
                        pass



    def handle_client(self, threadclientsocket):
        # try:
        while (1):
            # read request from client
            req = sendlib.read_socket(threadclientsocket)
            if (req is None):
                threadclientsocket.close()
                break
            print req
            jp = jsonParser(req)
            operation = jp.getValue("operation")
            if operation == "CREATE":
                isclientrequest = False
                filename = jp.getValue("file_name")
                actualfilename = filesendlib.actualfilename(filename)
                if (jp.has("token")):
                    requestToken = jp.getValue("token")
                    if requestToken in self.requesttokens:
                        print ("sending terminate")
                        sendlib.write_socket(threadclientsocket, "terminate")
                        continue
                    else:
                        self.requesttokens.add(requestToken)
                        sendlib.write_socket(threadclientsocket, "continue")
                else:
                    isclientrequest = True
                    requestToken = random.getrandbits(128)
                    reqdic = jp.getdic()
                    reqdic["token"] = requestToken
                    self.requesttokens.add(requestToken)
                    self.store_meta_memory(actualfilename, jp)
                # hopcount- no of hops the actual file must be farwarded
                updatedhopcount = self.gethopcount(jp)
                if updatedhopcount >= 0:
                    req = self.updatehopcountrequest(jp, updatedhopcount)
                self.create(filename, req, threadclientsocket, updatedhopcount, isclientrequest)
                if filename == actualfilename:
                    if isclientrequest:
                        if zk.exists("metadata/" + filename) is None:
                            zk.create("metadata/" + filename, str(self.meta[filename]), ephemeral=False, makepath=True)
            elif operation == "READ":
                filename = jp.getValue("file_name")
                self.read(filename, threadclientsocket)
            elif operation == "LIST":
                self.list(threadclientsocket)
            elif operation == "META":
                filename = jp.getValue("file_name")
                filemeta = self.read_meta(filename)
                if filemeta is not None:
                    sendlib.write_socket(threadclientsocket, str(self.meta[filename]))
                else:
                    sendlib.write_socket(threadclientsocket, "ENOENT")
            elif operation == "EXIT":
                self.close()
                break





                # except Exception as e:
                #     print str(e)

    def close(self):
        self.serversocket.close()


def cleanup(zk):
    print("performing cleanup")
    zk.delete("dedupequeue", recursive=True)
    zk.create("dedupequeue", "somevalue")
    zk.delete("owner", recursive=True)
    zk.delete("metadata", recursive=True)
    zk.create("metadata", "somevalue")







if __name__ == '__main__':
    logging.basicConfig()

    # storage_path=raw_input("enter server name")


    root_path = "/root"
    leader_path = root_path + "/leader"
    if (len(sys.argv)<=2):
    	peer_port = random.randrange(49152, 65535)
    else:
	peer_port = int(sys.argv[2])

    #zk = KazooClient(hosts='152.46.16.201:2181')
    zk = KazooClient(hosts=sys.argv[1]+":2181")

    zk.start()
    if (len(sys.argv)<=3) or (not (sys.argv[3]=="true")):
	    try:
		l = zk.get_children(root_path)
		if len(l) == 0:
		    cleanup(zk)
	    except NoNodeError:
		cleanup(zk)

    print("started with port", peer_port)

    storage_path = str(peer_port)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    host = socket.gethostbyname(socket.gethostname())

    e = election.election(zk, leader_path, host + "," + str(peer_port))

    meta = {}





    s1 = server(host, peer_port, storage_path, e, meta, zk)

    e.perform()


    print("this is a dedupeserver")
    t = threading.Thread(target=s1.performdedupe)
    t.daemon = True
    t.start()

    while True:
        c = s1.accept()
        t = threading.Thread(target=s1.handle_client, args=(c,))
        t.daemon = True
        t.start()

    s1.close()
    zk.stop()
    zk.close()
