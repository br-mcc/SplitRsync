#!/usr/bin/env python

class RsyncSession:
    def __init__(self, options, shell, largefile, splitter):
        self.options = options
        self.rShell = shell
        self.largefile = largefile
        self.splitter = splitter
        self.rShell.process = 'rsync'
        self.file = self.largefile.basename
        self.host = self.options.remotehost
        self.hostpath = self.options.remotepath
        self.chunkdir = self.options.chunkdir
        self.totalPieces = self.getlocalcount()
        self.numPieces = 0
        self.fileset = ''
        self.progress = 0
        self.synch_queue = 0

    def checkfile(self):
        self.rShell.cmd = "ssh -qq %s 'ls -l %s/%s'|wc -l 2> /dev/null" % (self.host, self.hostpath, self.file)
        self.rShell.flag = 1
        if int(self.rShell.runbash()) == 1:
            return True
        elif int(self.rShell.runbash()) == 0:
            return False

    def waittocomplete(self):
        while self.rShell.getqueue() == 1:
            self.updateprogress()
            time.sleep(2)

    def callrsync(self):
        ''' Build Rsync command and create rShell process.'''
        source = self.file+'_'+self.fileset+'*'
        self.rShell.cmd = 'rsync -rlz --include "%s" --exclude "*" %s/ %s/ 2> /dev/null &' % (source, self.chunkdir, self.options.destination)
        self.rShell.flag = 0
        self.rShell.runbash()

    def getlocalcount(self):
        self.rShell.cmd = 'ls -l %s/%s_*|wc -l 2> /dev/null' % (self.chunkdir, self.file)
        self.rShell.flag = 1
        count = int(self.rShell.runbash())
        return count

    def getremotecount(self):
        ''' Check remote system for completed file transfers.'''
        self.rShell.cmd = "ssh -qq %s 'ls -l %s/%s_*|wc -l 2> /dev/null'" % (self.host, self.hostpath, self.file)
        self.rShell.flag = 1
        count = int(self.rShell.runbash())
        return count

    def updateprogress(self):
        self.rShell.current = self.getremotecount()
        self.rShell.total = self.getlocalcount()
        self.rShell.printprogress('Transferring: ')