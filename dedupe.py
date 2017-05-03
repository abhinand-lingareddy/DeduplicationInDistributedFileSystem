import os
import hashlib


class dedupewritevariables():
    def __init__(self):
        self.chunk = []
        self.buffer = []
        self.writePointer = 0
        self.readPointer = 0
        self.dataSize = 0


class deduplication():
    def __init__(self, windowSize=10, Q=11497, D=256, boundary_marker=10, boundary=1, dedupepath=""):
        self.windowSize=windowSize#10
        self.Q=Q#11497
        self.D=D#256
        self.boundary_marker=boundary_marker
        for i in range(0, boundary_marker):
            boundary *= 2
        boundary -= 1
        self.boundary=boundary
        self.pow=self.compute_RM(self.windowSize, self.D, 1, self.Q)
        if dedupepath!="":
            self.dedupepath=dedupepath+"//"
        else:
            self.dedupepath = dedupepath

    def compute_RM(self,len, R, RM, Q):
        for i in range(1, len):
            RM = (R * RM) % Q
        return RM

    def PushChar(self,c,dv):
        dv.writePointer += 1
        if (dv.writePointer >= self.windowSize):
            dv.writePointer = 0
        dv.buffer.insert(dv.writePointer, c)
        dv.dataSize += 1
        dv.chunk.append(c)
        return c

    def PopChar(self,dv):
        dv.readPointer += 1
        if (dv.readPointer >= self.windowSize):
            dv.readPointer = 0
        dv.dataSize -= 1
        return dv.buffer[dv.readPointer]

    def createHashPath(self,hash):
        path = ""
        path += self.dedupepath
        path += hash[0:2]
        path += "//"
        path += hash[2:6]
        path += "//"
        path += hash[6:14]
        path += "//"
        path += hash[14:]

        return path

    def createChunkFile(self,chunk, hash):
        hashPath = self.createHashPath(hash)
        if not os.path.exists(hashPath):
            os.makedirs(hashPath)
            chunkFilename = hashPath + "//data.txt"
            file = open(chunkFilename, "w+")
            file.write(''.join(chunk))
            file.close()
            refFilename = hashPath + "//links.txt"
            file = open(refFilename, "w+")
            file.write(str(1))
            file.close()
        else:
            refFilename = hashPath + "//links.txt"
            file = open(refFilename, "r")
            count = int(file.read())
            count += 1
            file.close()
            file = open(refFilename, "w")
            file.write(str(count))
            file.close()

    def processChunk(self,currentFile,start, end,chunk):
        hash = hashlib.sha1(''.join(chunk).encode('utf-8')).hexdigest()
        self.createChunkFile(chunk, hash)
        tempFile = self.dedupepath+currentFile + "._temp"
        content = str(start) + " " + str(end) + " " + str(end - start) + " " + str(hash) + "\n"
        file = open(tempFile, "a+")
        file.write(content)
        file.close()

    def read(self,currentFile):
        hashfile = self.dedupepath+currentFile + "._temp"
        hf = open(hashfile, "r")
        actualdata=[]
        while True:
            line = hf.readline()
            if not line:
                break
            arr = line.split(" ")
            hash = arr[3].strip('\n')
            path = self.createHashPath(hash)
            path += "//data.txt"
            data = open(path, "r")
            actualdata.append(data.read())
            data.close()
        return "".join(actualdata)

    def write(self,currentFile):
        sig=0
        f = open(self.dedupepath+currentFile, 'r')
        dv=dedupewritevariables()
        for i in range(0, self.windowSize):
            c = f.read(1)
            self.PushChar(c,dv)
            sig = (sig * self.D + ord(c)) % self.Q

        # print("sig value = ", sig)

        index = 0
        lastIndex = 0

        while True:
            c = f.read(1)
            if c:
                s = self.PopChar(dv)
                # print("sig = ", sig, pow, s, Q)
                sig = (sig + self.Q - self.pow * ord(s) % self.Q) % self.Q
                s = self.PushChar(c,dv)
                sig = (sig * self.D + ord(s)) % self.Q
                index += 1
                if ((sig & self.boundary) == 0):
                    if (index - lastIndex >= 2048):
                        print("sig & boundary", sig, lastIndex, index, index - lastIndex)
                        print("chunk size =", index - lastIndex)
                        self.processChunk(currentFile,lastIndex, index,dv.chunk)
                        dv.chunk=[]
                        lastIndex = index
                elif (index - lastIndex >= 65536):
                    # print("sig & boundary", sig, index, index-lastIndex)
                    print("chunk size =", index - lastIndex)
                    self.processChunk(currentFile,lastIndex, index,dv.chunk)
                    dv.chunk=[]
                    lastIndex = index
            else:
                if lastIndex<index:
                    print("last chunk size =", index - lastIndex)
                    self.processChunk(currentFile, lastIndex, index, dv.chunk)
                    break


        print("Index =", index, "chunk size =", index - lastIndex)
        f.close()
        return 1
