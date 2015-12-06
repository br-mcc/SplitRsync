#!/usr/bin/env python

class Builder:
    def __init__(self, shell, session, largefile):
        self.buildershell = shell
        self.session = session
        self.localfilesize = largefile.size
        self.localsum = largefile.checksum
        self.remotesum = ''

    def cat(self):
        self.buildershell.cmd = "ssh %s 'cd %s; cat %s_* > %s  &'" % (self.session.host, self.session.hostpath, self.session.file, self.session.file)
        self.buildershell.flag = 0
        self.buildershell.runbash()
        self.buildershell.progress=0

    def getremotesize(self):
        self.buildershell.cmd = "ssh -qq %s 'ls -l %s/%s'|awk '{print $5}'" % (self.session.host,  self.session.hostpath, self.session.file)
        self.buildershell.flag = 1
        remotefilesize = self.buildershell.runbash()
        return int(remotefilesize)

    def progress(self):
        self.buildershell.current = self.getremotesize()
        self.buildershell.total = self.localfilesize
        self.buildershell.printprogress('Building: ')

    def comparesums(self):
        self.buildershell.cmd = "ssh -qq %s 'md5sum %s/%s'|awk '{print $1}'" % (self.session.host, self.session.hostpath, self.session.file)
        self.buildershell.flag = 1
        self.remotesum = self.buildershell.runbash()
        print '''
Local sum:  %s
Remote sum: %s''' % (self.localsum,  self.remotesum)
        if str(self.remotesum).strip() != str(self.localsum).strip():
            print 'ERROR:  Checksums don\'t match!  There might have been a problem during the file transfer!'
            sys.exit(0)
        else:
            print 'Transfer and rebuild of file succeeded!'

    def clean(self):
        self.buildershell.cmd = "ssh -qq %s find %s -name '%s_*' -exec rm -f {} \;" % (self.session.host, self.session.hostpath, self.session.file)
        self.buildershell.flag = 0
        self.buildershell.runbash()
        self.buildershell.cmd = "ssh -qq %s 'ls -l %s'" % (self.session.host, self.session.hostpath)
        self.buildershell.flag = 1
        contents = self.buildershell.runbash()
        print '''
    Remote directory contents after cleaning...
    %s''' % contents