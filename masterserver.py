
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
    def __init__(self,host,port,storage_path,elect):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path
        self.elect=elect

    def accept(self):
        c, addr =self.serversocket.accept()
        self.clientsocket=c

    def on_child_sucess1(self):
        if not self.elect.getmaster():
            sendlib.write_socket(self.clientsocket, "sucess2")
        else:
            response = self.prepare_response(200)
            sendlib.write_socket(self.clientsocket, response)

    def writetochild(self,storage_path,filename,req):
        while(True and self.elect.child is not None):
            try:
                filesendlib.sendfile(storage_path + filename, self.elect.child.s, req)
                response = sendlib.read_socket(self.elect.child.s)
                if response=="sucess1":
                    break
            except socket.error:
                time.sleep(60)
        return "sucess1"

    def create(self,filename,req):
        filesendlib.recvfile(self.storage_path,filename,self.clientsocket)
        if not self.elect.getmaster():
            sendlib.write_socket(self.clientsocket,"sucess1")

        if self.elect.child is not None:
            storage_path = filesendlib.storagepathprefix(self.storage_path)
            response=self.writetochild(storage_path,filename,req)
            if response=="sucess1":
                self.on_child_sucess1()
            while True and self.elect.child is not None:
                    try:
                        response = sendlib.read_socket(self.elect.child.s)
                        if response == "sucess2":
                            break
                    except socket.error:
                        time.sleep(60)
                        response = self.writetochild(storage_path, filename, req)
        else:
            self.on_child_sucess1()


    def read(self,filename):
        response=self.prepare_response(200)
        if filesendlib.sendfile(self.storage_path+"/"+filename,self.clientsocket,response):
            return None
        else:
            return 404

    def list(self):
        result=self.response_dic(200)
        if os.path.exists(storage_path):
            result["files"] = os.listdir(self.storage_path)
        else:
            result["files"] = []
        sendlib.write_socket(self.clientsocket,str(result))


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



    def handle_client(self):
        try:
            while(1):
                req = sendlib.read_socket(self.clientsocket)
                print req
                jp=jsonParser(req)
                operation=jp.getValue("operation")


                if operation=="CREATE":
                    filename = jp.getValue("file_name")
                    result=self.create(filename,req)
                elif operation=="READ":
                    filename = jp.getValue("file_name")
                    result=self.read(filename)
                elif operation=="LIST":
                    result = self.list()
                elif operation=="EXIT":
                    self.close()
                    break


                if result is not None:
                    response=self.prepare_response(result)
                    sendlib.write_socket(self.clientsocket,response)
        except Exception as e:
            print str(e)



    def close(self):
        self.clientsocket.close()



if __name__ == '__main__':

    #storage_path=raw_input("enter server name")

    leader_path="/leader"

    peer_socket = random.randrange(49152, 65535)

    zk = KazooClient(hosts='127.0.0.1:2181')

    zk.start()

    print "started with port",peer_socket

    storage_path=str(peer_socket)

    host=socket.gethostname()


    e = election.election(zk, leader_path,host+"," +str(peer_socket))

    s1=server(host,peer_socket,storage_path,e)



    e.perform()


    while True:
        s1.accept()
        t = threading.Thread(target=s1.handle_client)
        t.daemon = True
        t.start()

    s1.close()
    zk.stop()
    zk.close()


