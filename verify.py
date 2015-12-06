#!/usr/bin/env python

class Verifier:
    def __init__(self, shell, session):
        self.vShell = shell
        self.vSession = session
        self.vShell.process = 'rsync'
        self.locallist = []
        self.remotelist = []
        self.set =''
        self.vShell.total = session.getlocalcount()

    def fetchlist(self, listType):
        if 'local' in listType:
            self.vShell.cmd = "cd %s; ls  -l %s_%s*|awk '{print $5, $NF}'" % (self.vSession.chunkdir, self.vSession.file, self.set)
        else:
            self.vShell.cmd = "ssh %s 'cd %s; ls -l %s_%s*'|awk '{print $5, $NF}'" % (self.vSession.host, self.vSession.hostpath, self.vSession.file, self.set)
        self.vShell.flag = 1
        return self.vShell.runbash()

    def comparefiles(self):
        self.vShell.current = 0
        for letter in ascii_lowercase:
            self.set = letter
            self.locallist = self.fetchlist('local')
            if self.locallist == '':
                break
            self.remotelist = self.fetchlist('remote')

            while self.locallist != self.remotelist:
                self.vSession.fileset = letter
                self.vSession.callrsync()
                while self.vShell.getqueue() == 1:
                    time.sleep(1)

            self.vShell.current =  self.vShell.current + self.locallist.count('\n')
            self.vShell.printprogress('Verifying: ')