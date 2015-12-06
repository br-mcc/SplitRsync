#!/usr/bin/env python

class Splitter():
    def __init__(self, options, shell, largefile):
        self.options = options
        self.sShell = shell
        self.largefile = largefile
        self.sShell.process = 'split'
        self.file = largefile.file
        self.basename = largefile.basename
        self.filesize =  largefile.size
        self.chunksize = self.options.chunksize
        self.chunkdir = self.options.chunkdir
        print "chunksize: ", self.chunksize
        self.numPieces = 0

    def precheck(self, session):
        if session.getlocalcount() != 0:
            print 'Chunk directory not empty.'
            check = raw_input('     Wipe the directory?[y/n]: ')
            if check == 'y' or check == 'yes':
                self.sShell.cmd = 'rm -rvf %s/*' % (self.options.chunkdir)
                self.sShell.flag = 0
                self.sShell.runbash()
                return True  # Continue with splitting
            else:
                print 'Skip splitting and continue with file transfer?'
                check = raw_input('     [y] to start transfer,  [n] to exit:[y/n]: ')
                if check == 'y' or check == 'yes':
                    return False # Don't split. Start transfer.
                else:
                    print "Clear directory and rerun.  Exiting . . ."
                    sys.exit(0)
        else:
            return True

    def calcpieces(self):
        # Given a non-default size option,  calculate number of chunks.
        self.numPieces = float(self.filesize) / float(self.chunksize * 1024 * 1024)
        if self.numPieces / round(self.numPieces) != 1:
            self.numPieces = math.ceil(self.numPieces)
        print ">>>>> Estimated number of chunks of size", self.chunksize, "MB: ",  self.numPieces
        # Too many pieces will be created.  Warn user and exit.
        if self.numPieces > 676:
            self.chunksize = self.options.calcpiecesize()
            print "Error: Option '-b' too small.  Too many chunks will be created."
            print "       >>>>>> Try a value of (x) where: ", self.chunksize, " < x < 1024"
            print ""
            sys.exit(0)
        else:
            cont = raw_input(">>>>> Is this reasonable?  Would you like to continue? [y/n]: ")
            if 'n' in cont:
                sys.exit(0)
            print ""

    def split(self):
        # Calculate number of pieces and prompt to continue.
        # Warn user if number of chunks exceeds number of chunk suffix combinations ("file.tar.gz_zz")
        print '''
Calculating number of chunks with given chunk size.'''
        self.calcpieces()

        self.basename = self.largefile.basename
        self.path = self.chunkdir+'/'+str(self.basename)

        self.sShell.cmd = 'split -b %sm %s %s_ &' % (str(self.chunksize), self.file, self.path)
        self.sShell.flag = 0
        self.sShell.runbash()
        time.sleep(1)

        # Print progress of split command.
        self.sShell.cmd = 'ls -l %s* |wc -l 2> /dev/null' % (self.path)
        self.sShell.flag = 1
        self.sShell.total = self.numPieces
        while self.sShell.current < self.numPieces:
            self.sShell.current = int(self.sShell.runbash())
            self.sShell.printprogress('Splitting: ')