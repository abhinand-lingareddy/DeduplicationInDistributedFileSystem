import socket
import filesendlib
import sendlib
from time import time
from stat import  S_IFREG
from myparser import jsonParser
"""
class used by server/console/fuse for making calls to other servers 
"""
class client:
    def __init__(self,host,port):
        self.s = socket.socket()
        self.s.settimeout(300)
        self.s.connect((host, port))


    def sendcreaterequest(self, file_name):
        request=self.createrequest(file_name)
        meta=dict(st_mode=S_IFREG, st_nlink=1,
                               st_size=0, st_ctime=time(), st_mtime=time(),
                               st_atime=time())
        request["meta"] = meta
        sendlib.write_socket(self.s, str(request))
        return meta
    @staticmethod
    def createrequest(file_name):
        request = {}
        request["file_name"] = file_name
        request["operation"] = "CREATE"
        return request

    def metadataoperation(self, filename):
        request = {}
        request["file_name"] = filename
        request["operation"] = "META"
        sendlib.write_socket(self.s, str(request))
        response = sendlib.read_socket(self.s)
        print "meta"+response
        if response=="ENOENT":
            return None
        meta = jsonParser(response)
        return meta.getdic()



    def readresponse(self):
        response = sendlib.read_socket(self.s)
        print response

    def createoperation(self, file_name, file_path):
        self.sendcreaterequest(file_name)
        filesendlib.send_fromactual(file_path, self.s)
        self.readresponse()

    def sendreadrequestandgetresponse(self, file_name):
        request = {}
        request["file_name"] = file_name
        request["operation"] = "READ"
        sendlib.write_socket(self.s, str(request))
        response = sendlib.read_socket(self.s)
        jp = jsonParser(response)
        return jp

    def sendreadrequestandgetmeta(self, file_name):
        jp = self.sendreadrequestandgetresponse(file_name)
        return jp.getValue("meta")

    def sendreadrequestandgetstatus(self, file_name):
        jp=self.sendreadrequestandgetresponse(file_name)
        return jp.getValue("status")

    def openforread(self,file_name,path):

        if self.sendreadrequestandgetstatus(file_name)==200:
            f = filesendlib.getfilepointer(path, file_name)
        else:
            f=None
        return f

    def read(self,file_name,path):
        f=self.openforread(file_name,path)
        if f is not None:
            #filesendlib.recvfile(None,file_name,self.s)
            filesendlib.recvfilewithpointer(f,self.s)
        else:
            print "file not found in the server"

    def listoperation(self):
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
    port = 49548

    c=client(host,port)

    while(1):
        try:
            print("1.Create")
            print("2.Read")
            print("3.List")
            print("4.meta")
            print("5.Exit")
            x = input("Enter a number")

            if(x==1):
                file_name=raw_input("enter filename")
                file_path=raw_input("enter filepath")
                c.createoperation(file_name, file_path)
            elif(x==2):
                file_name=raw_input("enter filename")
                c.read(file_name,None)
            elif(x==3):
                print c.listoperation()
            #exit operation
        except NameError:
            print "invalid inputs"

    c.close()

