import sendlib
import os
import socket



def recvfile(storage_path,filename,s):
    if storage_path is not None:
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        f = open(storage_path + '/' +filename , 'wb')
    else:
        f = open(filename, 'wb')
    print "Receiving..."
    l = s.recv(1024)
    while (l):
        f.write(l)
        print "Receiving..."
        l = s.recv(1024)
    f.close()
    print "Done Receiving"



def sendfile(file_path,s,ack):
    if not os.path.isfile(file_path):
        pass

    f = open(file_path, 'rb')
    sendlib.write_socket(s,ack)
    l = f.read(1024)
    while (l):
        print 'Sending...'
        s.send(l)
        l = f.read(1024)
    f.close()
    print "Done Sending"
    s.shutdown(socket.SHUT_WR)
    return True