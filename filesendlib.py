import sendlib
import os
import socket

def no_to_bytes(no):
    b=str(no)
    non_zeroscounter=len(b)
    zeros=[]
    for i in range(4-non_zeroscounter):
        zeros.append("0")
    return "".join(zeros)+b


def bytes_to_no(b):
    return int(b)

def storagepathprefix(storage_path):
    if storage_path is not None:
        return storage_path + '/'
    else:
        return ""

def getfilepointer(storage_path,filename):
    storage_path = storagepathprefix(storage_path)
    if len(storage_path) > 0:
        f = open(storage_path + filename, 'wb')
    else:
        f = open(filename, 'wb')
    return f
def recvfile(storage_path,filename,s):
    # length = s.recv(4)
    # length = bytes_to_no(length)
    f=getfilepointer(storage_path,filename)
    recvfilewithpointer(f,s)

def recvfilewithpointer(f,s):
    while (True):
        print "Receiving..."
        l = s.recv(1024)
        if l[-1]=='~':
            f.write(l[:-1])
            break
        f.write(l)
    f.close()
    print "Done Receiving"



def send_file(file_path, s):
    # length=os.stat(file_path).st_size
    # length_str = no_to_bytes(length)
    # s.send(length_str)

    f = open(file_path, 'rb')
    while (True):
        l = f.read(1024)
        if len(l)<1024:
            l=l+'~'
            s.send(l)
            print 'Sending done...'
            break
        s.send(l)
        print 'Sending'
    f.close()
    print "Done Sending"
    #s.shutdown(socket.SHUT_WR)

def sendmetadataandfile(file_path, s, ack):
    sendlib.write_socket(s, ack)
    send_file(file_path, s)


