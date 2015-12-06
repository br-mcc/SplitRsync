#!/usr/bin/env python

class LargeFile:
    def __init__(self, options, shell):
        self.options = options
        self.file = options.file
        self.lShell = shell
        self.checksumfile = ''
        self.checksum = self.getlocalsum()
        self.basename = ''
        self.size = self.getfilesize()
        self.progress = 0
        self.exists = 0

    def fileexists(self, filedir):
        filedir = filedir
        self.lShell.cmd = 'ls -ld %s|wc -l 2> /dev/null' % (filedir)
        self.lShell.flag = 1
        self.exists = int(self.lShell.runbash())

    def getbasename(self):
        print "File: ",  self.file
        return self.file.rsplit('/', 1)[1]

    def getfilesize(self):
        filestat = os.stat(self.file)
        self.size = filestat.st_size
        return self.size

    def fetchpath(self):
        self.lShell.cmd = 'pwd'
        self.lShell.flag = 1
        path = self.lShell.runbash()
        # Return path as string stripped of special characters.
        return str(path).strip()

    def getlocalsum(self):
        print "\nFetching local file's checksum..."
        self.checksumfile = self.options.file+'.md5sum'
        print 'MD5Sum File: ', self.checksumfile
        try:
            f = open(self.checksumfile,'r')
            localsum = f.readline()
            f.close()
        except:
            self.lShell.cmd = "md5sum %s|awk '{print $1}'" % (self.file)
            self.lShell.flag = 1
            localsum = self.lShell.runbash()
            f = open(self.checksumfile,'w+')
            f.write(str(localsum))
            f.close
        return localsum