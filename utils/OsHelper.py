import os

class OsOperation:
    def __init__(self):
        pass

    def deleteFile(self,filePath):
        if filePath:
            os.system("rm -rf %s" % filePath)
