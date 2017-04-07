
from myparser import jsonParser
import socket
import sendlib
import threading
import filesendlib
import random
from client import client
import kazoo
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.recipe.election import Election

class server:
    def __init__(self,host,port,storage_path,slaves):
        s = socket.socket()
        s.bind((host, port))
        s.listen(5)
        self.serversocket=s
        self.storage_path=storage_path
        self.master=False
        self.slaves=slaves

    def set_master(self):
        self.master=True

    def accept(self):
        c, addr =self.serversocket.accept()
        self.clientsocket=c


    def create(self,filename):
        if self.master:
            filesendlib.recvfileredirect(self.storage_path, filename, self.clientsocket,self.slaves)
        else:
            filesendlib.recvfile(self.storage_path, filename, self.clientsocket)
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



    def handle_client(self,slaves):
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


def stat_listener(state):
    if state == KazooState.LOST:
        print 'Lost'
    elif state == KazooState.SUSPENDED:
        print 'Suspended'
    elif state == KazooState.CONNECTED:
        print 'Connected'
    else:
        print 'Unknown state'

def addSlaves(slaves,s,zk,sevport):
    for sub in s:
        if sub!=sevport:
            print "trying to add",sub
            hostkey = "/config/host/"+sub
            print "checkpoint 1"
            host,stat=zk.get(hostkey)
            portkey="/config/port/"+sub
            port,stat=zk.get(portkey)
            print "checkpoint 2"
            slaves[sub]=client(host,int(port))
            print "printing after checkpoint",slaves

def removeSlaves(slaves, s,sevport):
    for sub in s:
        if sevport !=sub:
            print "removing " + sub
            c=slaves[sub]
            c.close()
            del slaves[sub]

#what if slaves dies
#what if new slaves comes
def leader(serv,zk,slaves,port):
    print "I am the leader"
    global s
    s=set()
    serv.set_master()
    @kazoo.client.ChildrenWatch(zk, '/config/host')
    def my_func(children):
        print "Children are %s" % children
        cset = set(children)
        global s
        n = cset - s
        if len(n)>0:
            addSlaves(slaves, n, zk,port)
        n = s - cset
        if len(n)>0:
            removeSlaves(slaves, n,port)
        s = cset
    while(1):
        #use this thread
        pass





if __name__ == '__main__':

    #storage_path=raw_input("enter server name")

    leader_path="/leader"

    peer_socket = random.randrange(49152, 65535)

    zk = KazooClient(hosts='127.0.0.1:2181')

    zk.add_listener(stat_listener)

    zk.start()

    print "started with port",peer_socket

    storage_path=str(peer_socket)

    host=socket.gethostname()

    slaves = {}

    s1=server(host,peer_socket,storage_path,slaves)

    zk.create("/config/host/"+storage_path,host,ephemeral=True,makepath =True)

    zk.create("/config/port/"+storage_path , str(peer_socket), ephemeral=True,makepath=True)

    if not zk.exists(leader_path):
        zk.create(leader_path,makepath=True)

    elect = Election(zk,leader_path,storage_path)

    t = threading.Thread(target=elect.run, args=(leader,s1,zk,slaves,str(peer_socket)))
    t.daemon = True
    t.start()

    while True:
        s1.accept()
        t = threading.Thread(target=s1.handle_client,args=(slaves,))
        t.daemon = True
        t.start()

    s1.close()
    zk.stop()
    zk.close()


