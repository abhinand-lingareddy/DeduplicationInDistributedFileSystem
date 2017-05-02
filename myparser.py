
class jsonParser:

    def __init__(self,st):
        self.request_json = eval(st)

    def getValue(self,key):
        if key in self.request_json:
            return self.request_json[key]
        else:
            return None
    def getdic(self):
        return self.request_json

    def has(self,key):
        return key in self.request_json



