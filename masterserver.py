
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib


def create(param):
    s,storage_path,filename=param
    filesendlib.recvfile(storage_path,filename,s)
    return (200,)

def read(param):
    s, storage_path, filename = param
    response={}
    response["status"]=200
    if filesendlib.sendfile(storage_path+"/"+filename,s,str(response)):
        return (200,)
    else:
        return (404,)

operationmap={
    "CREATE": create,
    "READ": read

}

def prepare_response(result):
    code = {
        200:
            "OK",
        400:
            "Bad Request",
        404:
            "Not Found",
    }

    status=result[0]
    response={}
    response["status"]=status
    response["message"]=code[status]

    return str(response)



def handle_client(c,addr,storage_path):
    req = sendlib.read_socket(c)
    print req
    jp=jsonParser(req)
    operation=jp.getValue("operation")
    filename=jp.getValue("file_name")

    result=operationmap[operation]((c,storage_path,filename))

    response=prepare_response(result)

    sendlib.write_socket(c,response)

    c.close()







if __name__ == '__main__':

    #storage_path=raw_input("enter server name")

    storage_path="1"
    s=socket.socket()
    host=socket.gethostname();

    port=7734
    s.bind((host, port))

    s.listen(5)

    while True:
        c, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(c, addr,storage_path))
        t.daemon = True
        t.start()


