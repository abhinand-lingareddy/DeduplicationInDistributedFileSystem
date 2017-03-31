import socket
import filesendlib
import sendlib
from myparser import jsonParser

class client:
    def __init__(self,host,port):
        self.s = socket.socket()
        self.s.connect((host, port))


    def create(self,file_name,file_path):
        request={}
        request["file_name"]=file_name
        request["operation"]="CREATE"

        if not filesendlib.sendfile(file_path,self.s,str(request)):
            print "file not found"
        else:
            response=sendlib.read_socket(self.s)
            print response

    def read(self,file_name):
        request = {}
        request["file_name"] = file_name
        request["operation"] = "READ"
        sendlib.write_socket(self.s,str(request))
        response=sendlib.read_socket(self.s)
        jp=jsonParser(response)
        if jp.getValue("status")==200:
            filesendlib.recvfile(None,file_name,self.s)
        else:
            print "file not found in the server"

    def close(self):
        self.s.close()




if __name__ == '__main__':
    host = socket.gethostname()
    port = 7734
    c=client(host,port)

    while(1):
        print("1.Create")
        print("2.Read")
        print("5.Exit")
        x = input("Enter a number")

        if(x==1):
            file_name=raw_input("enter filename")
            file_path=raw_input("enter filepath")
            c.create(file_name,file_path)
        elif(x==2):
            file_name=raw_input("enter filename")
            c.read(file_name)
        #exit operation

    c.close()
