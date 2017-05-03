
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
import random
from kazoo.client import KazooClient
from client import client
import election
import time
import os
import dedupe

class server:
    def __init__(self,host,port,storage_path,elect,meta):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path
        self.elect=elect
        self.meta=meta
        self.no_dedupe_servers=2
        self.ds=dedupe.deduplication(dedupepath=storage_path)

    def accept(self):
        c, addr =self.serversocket.accept()
        return c

    def getchildclient(self):
        if self.elect.childinfo is not None:
            host = self.elect.childinfo[:self.elect.childinfo.index(',')]
            port = int(self.elect.childinfo[self.elect.childinfo.index(',') + 1:])
            return client(host, port)

    def on_child_sucess1(self,threadclientsocket):
        if not self.elect.getmaster():
            sendlib.write_socket(threadclientsocket, "sucess2")
        else:
            response = self.prepare_response(200)
            sendlib.write_socket(threadclientsocket, response)

    def writetochild(self,storage_path,filename,req,childclient):
        while(True and childclient is not None):
            try:
                filesendlib.sendrequestandfile(filesendlib.storagepathprefix(storage_path) , filename, childclient.s, req,self.ds)
                response = sendlib.read_socket(childclient.s)
                if response=="sucess1":
                    break
            except socket.error:
                time.sleep(60)
        return "sucess1"

    def create(self,filename,req,threadclientsocket,hopcount):
        filesendlib.recvfile(self.storage_path,filename,threadclientsocket)
        if not self.elect.getmaster():
            sendlib.write_socket(threadclientsocket,"sucess1")

        childclient=self.getchildclient()

        if hopcount>0 and childclient is not None:
            storage_path = filesendlib.storagepathprefix(self.storage_path)
            response=self.writetochild(storage_path,filename,req,childclient)
            if response=="sucess1":
                self.on_child_sucess1(threadclientsocket)
            while True and childclient is not None:
                    try:

                        response = sendlib.read_socket(childclient.s)
                        if response == "sucess2":
                            childclient.close()
                            break
                    except socket.error:
                        time.sleep(60)
                        response = self.writetochild(storage_path, filename, req,childclient)
        else:
            self.on_child_sucess1(threadclientsocket)


    def read(self,filename,threadclientsocket):
        response=self.response_dic(200)
        meta=self.read_meta(filename)
        if  meta is None:
            return 404
        response["meta"]=meta
        filesendlib.sendrequestandfile(filesendlib.storagepathprefix(self.storage_path), filename, threadclientsocket, str(response),self.ds)

    def list(self,threadclientsocket):
        result=self.response_dic(200)
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


    def write_meta(self,filename,jp):
        self.meta[filename]=jp.getValue("meta")
      



    def read_meta(self,filename):
        return self.meta[filename]







    def handle_client(self,threadclientsocket):
        # try:
            while(1):
                req = sendlib.read_socket(threadclientsocket)
                if(req is None):
                    threadclientsocket.close()
                    break
                print req
                jp=jsonParser(req)
                operation=jp.getValue("operation")
                if operation=="CREATE":
                    filename = jp.getValue("file_name")
                    self.write_meta(filename,jp)
                    if not jp.has("hopcount"):
                        hopcount=self.no_dedupe_servers
                    else:
                        hopcount=jp.getValue("hopcount")-1
                    if hopcount>0:
                        respdic=jp.getdic()
                        respdic["hopcount"]=hopcount
                        req=str(respdic)
                    self.create(filename,req,threadclientsocket,hopcount)

                    storagepath=filesendlib.storagepathprefix(self.storage_path)
                    size=os.path.getsize(storagepath+filename)
                    self.ds.write(filename)
                    os.remove(storagepath+filename)
                    self.meta[filename]["st_size"]=size
                    self.meta[filename]["st_ctime"]=time.time()
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



if __name__ == '__main__':

    #storage_path=raw_input("enter server name")

    leader_path="/leader"

    peer_socket = random.randrange(49152, 65535)

    zk = KazooClient(hosts='127.0.0.1:2181')

    zk.start()

    print "started with port",peer_socket

    storage_path=str(peer_socket)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)

    host=socket.gethostname()


    e = election.election(zk, leader_path,host+"," +str(peer_socket))

    meta={}

    s1=server(host,peer_socket,storage_path,e,meta)



    e.perform()


    while True:
        c=s1.accept()
        t = threading.Thread(target=s1.handle_client,args=(c,))
        t.daemon = True
        t.start()

    s1.close()
    zk.stop()
    zk.close()


