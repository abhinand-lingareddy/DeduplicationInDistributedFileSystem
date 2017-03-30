import socket
import filesendlib
import sendlib
from myparser import jsonParser

def create(s):
    request={}
    file_path=raw_input("enter file path")
    file_name = raw_input("enter file name")
    request["file_name"]=file_name
    request["operation"]="CREATE"

    if not filesendlib.sendfile(file_path,s,str(request)):
        print "file not found"
    else:
        response=sendlib.read_socket(s)
        print response

def read(s):
    request = {}
    file_name = raw_input("enter file name")
    request["file_name"] = file_name
    request["operation"] = "READ"
    sendlib.write_socket(s,str(request))
    response=sendlib.read_socket(s)
    jp=jsonParser(response)
    if jp.getValue("status")==200:
        filesendlib.recvfile(None,file_name,s)
        response = sendlib.read_socket(s)
        print response
    else:
        print "file not found in the server"



def consolethread():

    host = socket.gethostname()
    port = 7734


    options = {
        1:create,
        2:read
    }
    while (1):
        s = socket.socket()
        print("1.Create")
        print("2.Read")
        print("5.Exit")
        x=input("Enter a number")
        s.connect((host, port))
        data = options[x](s)
        s.close()

consolethread()