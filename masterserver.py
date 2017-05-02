
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
import random
import kazoo
from kazoo.client import KazooClient
import election
import time
import os

class server:
    def __init__(self,host,port,storage_path,elect,meta):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path
        self.elect=elect
        self.meta=meta

    def accept(self):
        c, addr =self.serversocket.accept()
        return c

    def on_child_sucess1(self,threadclientsocket):
        if not self.elect.getmaster():
            sendlib.write_socket(threadclientsocket, "sucess2")
        else:
            response = self.prepare_response(200)
            sendlib.write_socket(threadclientsocket, response)

    def writetochild(self,storage_path,filename,req):
        while(True and self.elect.child is not None):
            try:
                filesendlib.sendmetadataandfile(storage_path + filename, self.elect.child.s, req)
                response = sendlib.read_socket(self.elect.child.s)
                if response=="sucess1":
                    break
            except socket.error:
                time.sleep(60)
        return "sucess1"

    def create(self,filename,req,threadclientsocket):
        filesendlib.recvfile(self.storage_path,filename,threadclientsocket)
        if not self.elect.getmaster():
            sendlib.write_socket(threadclientsocket,"sucess1")

        if self.elect.child is not None:
            storage_path = filesendlib.storagepathprefix(self.storage_path)
            response=self.writetochild(storage_path,filename,req)
            if response=="sucess1":
                self.on_child_sucess1(threadclientsocket)
            while True and self.elect.child is not None:
                    try:
                        response = sendlib.read_socket(self.elect.child.s)
                        if response == "sucess2":
                            break
                    except socket.error:
                        time.sleep(60)
                        response = self.writetochild(storage_path, filename, req)
        else:
            self.on_child_sucess1(threadclientsocket)


    def read(self,filename,threadclientsocket):
        response=self.response_dic(200)
        meta=self.read_meta(filename)
        if  meta is None:
            return 404
        response["meta"]=meta
        filesendlib.sendmetadataandfile(self.storage_path + "/" + filename, threadclientsocket, str(response))

    def list(self,threadclientsocket):
        result=self.response_dic(200)
        result=self.response_dic(200)
        if os.path.exists(storage_path):
            files = [file for file in os.listdir(self.storage_path)
                     if os.path.isfile(os.path.join(self.storage_path, file))]
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
                    self.create(filename,req,threadclientsocket)
                    storagepath=filesendlib.storagepathprefix(self.storage_path)
                    size=os.path.getsize(storagepath+filename)
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

    #peer_socket = random.randrange(49152, 65535)
    peer_socket=50992

    zk = KazooClient(hosts='127.0.0.1:2181')

    zk.start()

    print "started with port",peer_socket

    storage_path=str(peer_socket)

    if not os.path.exists(storage_path):
        os.makedirs(storage_path)
        os.makedirs(storage_path+"/meta")

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


