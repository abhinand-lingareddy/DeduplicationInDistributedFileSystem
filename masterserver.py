
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
class server:
    def __init__(self,host,port,storage_path):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path

    def accept(self):
        c, addr =self.serversocket.accept()
        self.clientsocket=c


    def create(self,filename):
        filesendlib.recvfile(self.storage_path,filename,self.clientsocket)
        return 200

    def read(self,filename):
        response={}
        response["status"]=200
        if filesendlib.sendfile(self.storage_path+"/"+filename,self.clientsocket,str(response)):
            return None
        else:
            return 404


    @staticmethod
    def prepare_response(result):
        code = {
            200:
                "OK",
            400:
                "Bad Request",
            404:
                "Not Found",
        }

        status=result
        response={}
        response["status"]=status
        response["message"]=code[status]

        return str(response)



    def handle_client(self):
        while(1):
            req = sendlib.read_socket(self.clientsocket)
            print req
            jp=jsonParser(req)
            operation=jp.getValue("operation")
            filename=jp.getValue("file_name")

            if operation=="CREATE":
                result=self.create(filename)
            elif operation=="READ":
                result=self.read(filename)
            elif operation=="EXIT":
                self.close()
                break


            if result is not None:
                response=server.prepare_response(result)
                sendlib.write_socket(self.clientsocket,response)



    def close(self):
        self.clientsocket.close()




if __name__ == '__main__':

    #storage_path=raw_input("enter server name")

    storage_path="1"
    host=socket.gethostname();

    port=7734
    s1=server(host,port,storage_path)

    while True:
        s1.accept()
        t = threading.Thread(target=s1.handle_client)
        t.daemon = True
        t.start()


