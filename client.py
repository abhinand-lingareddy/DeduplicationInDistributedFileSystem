import socket
import filesendlib
import sendlib
import os
from time import time
from stat import S_IFDIR, S_IFLNK, S_IFREG
from myparser import jsonParser

class client:
    def __init__(self,host,port):
        self.s = socket.socket()
        self.s.connect((host, port))

    def sendmetadata(self,file_name):
        request = {}
        meta=dict(st_mode=S_IFREG, st_nlink=1,
                               st_size=0, st_ctime=time(), st_mtime=time(),
                               st_atime=time())
        request["meta"] = meta
        request["file_name"] = file_name
        request["operation"] = "CREATE"
        sendlib.write_socket(self.s, str(request))
        return meta



    def readresponse(self):
        response = sendlib.read_socket(self.s)
        print response

    def create(self,file_name,file_path):
        self.sendmetadata(file_name)
        filesendlib.send_file(file_path, self.s)
        self.readresponse()

    def openforreadresponse(self,file_name):
        request = {}
        request["file_name"] = file_name
        request["operation"] = "READ"
        sendlib.write_socket(self.s, str(request))
        response = sendlib.read_socket(self.s)
        jp = jsonParser(response)
        return jp
    def openforreadmeta(self,file_name):
        jp = self.openforreadresponse(file_name)
        return jp.getValue("meta")

    def openforreadstatus(self,file_name):
        jp=self.openforreadresponse(file_name)
        return jp.getValue("status")

    def openforread(self,file_name):

        if self.openforreadstatus(file_name)==200:
            f = filesendlib.getfilepointer(None, file_name)
        else:
            f=None
        return f

    def read(self,file_name):
        f=self.openforread(file_name)
        if f is not None:
            #filesendlib.recvfile(None,file_name,self.s)
            filesendlib.recvfilewithpointer(f,self.s)
        else:
            print "file not found in the server"

    def list(self):
        request= {}
        request["operation"] = "LIST"
        sendlib.write_socket(self.s, str(request))
        response = sendlib.read_socket(self.s)
        jp = jsonParser(response)
        if jp.getValue("status") == 200:
            files=jp.getValue("files")
        else:
            print response
            files=[]
        return files


    def close(self):
        self.s.close()




if __name__ == '__main__':
    host = socket.gethostname()
    port = 50992

    c=client(host,port)

    while(1):
        try:
            print("1.Create")
            print("2.Read")
            print("3.List")
            print("5.Exit")
            x = input("Enter a number")

            if(x==1):
                file_name=raw_input("enter filename")
                file_path=raw_input("enter filepath")
                c.create(file_name,file_path)
            elif(x==2):
                file_name=raw_input("enter filename")
                c.read(file_name)
            elif(x==3):
                print c.list()
            #exit operation
        except NameError:
            print "invalid inputs"

    c.close()

