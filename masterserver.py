
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
import random
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from client import client
import election
import time
import os
import dedupe
from threading import Lock


class server:
    def __init__(self,host,port,storage_path,elect,meta,zk):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path
        self.elect=elect
        self.meta=meta
        self.zk=zk
        self.no_dedupe_servers=2 #no of deduplication servers, used only when this is the master node
        self.isdedupeserver=False
        self.lock= Lock()
        self.ds=dedupe.deduplication(dedupepath=storage_path)

    #todo
    def getname(self):
        pass

    def accept(self):
        c, addr =self.serversocket.accept()
        return c

    def getchildclient(self):
        if self.elect.childinfo is not None:
            host = self.elect.childinfo[:self.elect.childinfo.index(',')]
            port = int(self.elect.childinfo[self.elect.childinfo.index(',') + 1:])
            return client(host, port)

    def on_child_sucess1(self,filename,threadclientsocket):
        if threadclientsocket is not None:
            if not self.elect.ismaster():
                sendlib.write_socket(threadclientsocket, "sucess2")
            else:
                self.zk.create("dedupequeue/" + filename, str(self.storage_path))
                response = self.prepare_response(200)
                sendlib.write_socket(threadclientsocket, response)

    def writetochild(self,storage_path,filename,req,childclient):
        while(True and childclient is not None):
            try:
                filesendlib.sendresponseandfile(filesendlib.storagepathprefix(storage_path), filename, childclient.s, req, self.ds)
                response = sendlib.read_socket(childclient.s)
                if response=="sucess1":
                    break
                else:
                    jp=jsonParser(response)
                    hashes=jp.getValue("hashes")
                    for hash in hashes:
                        data=self.ds.getdatafromhash(hash)
                        sendlib.write_socket(data,childclient.s)
            except socket.error:
                time.sleep(60)
                childclient=self.getchildclient()
        return "sucess1",childclient

    def handlechildwrite(self,filename,req,threadclientsocket,childclient):
        storage_path = filesendlib.storagepathprefix(self.storage_path)
        response,childclient = self.writetochild(storage_path, filename, req, childclient)
        if response == "sucess1":
            self.on_child_sucess1(filename, threadclientsocket)
        while True and childclient is not None:
            try:
                response = sendlib.read_socket(childclient.s)
                if response == "sucess2":
                    childclient.close()
                    break
            except socket.error:
                time.sleep(60)
                childclient=self.getchildclient()
                response,childclient = self.writetochild(storage_path, filename, req, childclient)

    def create(self,filename,req,threadclientsocket,hopcount):
        filesendlib.recvfile(self.storage_path,filename,threadclientsocket)
        if not self.elect.ismaster():
            if filename.endswith("._temp"):
                actualfilename=filesendlib.actualfilename(filename)
                if self.ds.actualfileexits(actualfilename):
                    self.ds.createchunkfromactualfile(filename,actualfilename)
                    sendlib.write_socket(threadclientsocket, "sucess1")
                else:
                    missingchunkhashes=self.ds.findmissingchunk(filename)
                    response={}
                    response["hashes"]=missingchunkhashes
                    sendlib.write_socket(threadclientsocket, str(response))
                    for missinghash in missingchunkhashes:
                        chunk=sendlib.read_socket(threadclientsocket)
                        self.ds.createChunkFile( chunk, missinghash)
            else:
                sendlib.write_socket(threadclientsocket,"sucess1")

        childclient=self.getchildclient()

        if hopcount>0 and childclient is not None:
            self.handlechildwrite(filename,req,threadclientsocket,childclient)
        else:
            self.on_child_sucess1(filename,threadclientsocket)





    def read(self,filename,threadclientsocket):
        response=self.response_dic(200)
        meta=self.read_meta(filename)
        if  meta is None:
            return 404
        response["meta"]=meta
        filesendlib.sendresponseandfile(filesendlib.storagepathprefix(self.storage_path), filename, threadclientsocket, str(response), self.ds)

    def list(self,threadclientsocket):
        result=self.response_dic(200)
        if os.path.exists(storage_path):
            files = [file for file in os.listdir(self.storage_path)
                     if os.path.isfile(os.path.join(self.storage_path, file))]
            for i in range(len(files)):
                if files[i].endswith("._temp"):
                    files[i]=files[i][:len("._temp")*-1]
            result["files"] = files
        else:
            result["files"] = []
        sendlib.write_socket(threadclientsocket,str(result))


    def response_dic(self,result):
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

    def prepare_response(self,result):
        response=self.response_dic(result)
        return str(response)


    def store_meta_memory(self, filename, jp):
        self.meta[filename]=jp.getValue("meta")
      



    def read_meta(self,filename):
        return self.meta[filename]




    def gethopcount(self,jp):
        if not jp.has("hopcount"):
            hopcount = self.no_dedupe_servers
        else:
            hopcount = jp.getValue("hopcount") - 1
        return hopcount
    def updatehopcountrequest(self,jp,hopcount):
        respdic = jp.getdic()
        respdic["hopcount"] = hopcount
        req = str(respdic)
        return req

    def updatesize(self,filename):
        storagepath = filesendlib.storagepathprefix(self.storage_path)
        size = os.path.getsize(storagepath + filename)
        self.meta[filename]["st_size"] = size
        self.meta[filename]["st_ctime"] = time.time()

    def performdedupe(self):
        while(True):
            pending_files=self.zk.get_children("dedupequeue")
            for file in pending_files:
                if self.ds.actualfileexits(file):
                    try:
                        if zk.exists("dedupeserver/"+file) is None:
                            zk.create("dedupeserver/"+file,str(self.storage_path),ephemeral=True,makepath=True)
                            print "deduping file " + file
                            self.ds.write(file)
                            request=client.createrequest(file+"._temp")
                            request["meta"]=self.meta[file]
                            request["hopcount"]=100
                            self.handlechildwrite(file+"._temp", str(request), None, self.getchildclient())
                            zk.delete("dedupequeue/"+file)
                    except NodeExistsError:
                        pass


    def handle_client(self,threadclientsocket):
        # try:
            while(1):
                #read request from client
                req = sendlib.read_socket(threadclientsocket)
                if(req is None):
                    threadclientsocket.close()
                    break
                print req
                jp=jsonParser(req)
                operation=jp.getValue("operation")
                if operation=="CREATE":
                    filename = jp.getValue("file_name")
                    actualfilename=filesendlib.actualfilename(filename)
                    self.store_meta_memory(actualfilename, jp)
                    #hopcount- no of hops the actual file must be farwarded
                    updatedhopcount=self.gethopcount(jp)
                    if updatedhopcount>=0 and updatedhopcount<99:
                        req=self.updatehopcountrequest(jp,updatedhopcount)
                        #extra check to avoid unnecessary locking
                        if not self.isdedupeserver and not self.elect.ismaster():
                            with self.lock:
                                if not self.isdedupeserver:
                                    print("this is a dedupeserver")
                                    self.isdedupeserver=True
                                    t = threading.Thread(target=self.performdedupe)
                                    t.daemon = True
                                    t.start()
                    self.create(filename,req,threadclientsocket,updatedhopcount)
                    self.updatesize(actualfilename)
                elif operation=="READ":
                    filename = jp.getValue("file_name")
                    self.read(filename,threadclientsocket)
                    self.meta[filename]["st_ctime"]=time.time()
                elif operation=="LIST":
                    self.list(threadclientsocket)
                elif operation=="META":
                    filename = jp.getValue("file_name")
                    if filename in self.meta:
                        sendlib.write_socket(threadclientsocket,str(self.meta[filename]))
                    else:
                        sendlib.write_socket(threadclientsocket,"ENOENT")
                elif operation=="EXIT":
                    self.close()
                    break


               


        # except Exception as e:
        #     print str(e)



    def close(self):
        self.serversocket.close()



def cleanup(zk):
    print "performing cleanup"
    zk.delete("dedupequeue", recursive=True)
    zk.create("dedupequeue", "somevalue")

if __name__ == '__main__':

    #storage_path=raw_input("enter server name")


    root_path="/root"
    leader_path=root_path+"/leader"

    peer_port = random.randrange(49152, 65535)

    zk = KazooClient(hosts='127.0.0.1:2181')

    zk.start()

    try:
        l=zk.get_children(root_path)
        if len(l)==0:
            cleanup(zk)
    except NoNodeError:
        cleanup(zk)

    print "started with port",peer_port

    storage_path=str(peer_port)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    host=socket.gethostname()


    e = election.election(zk, leader_path, host +"," + str(peer_port))

    meta={}

    s1=server(host, peer_port, storage_path, e, meta,zk)



    e.perform()


    while True:
        c=s1.accept()
        t = threading.Thread(target=s1.handle_client,args=(c,))
        t.daemon = True
        t.start()

    s1.close()
    zk.stop()
    zk.close()


