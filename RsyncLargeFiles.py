#!/usr/bin/python
import sys
from string import ascii_lowercase
import math
# Used for checking user/group permissions
import os
import pwd
import grp
import stat
# Used for issuing bash commands and collecting script options.
from datetime import datetime
import time
import getopt

# Custom Imports
from bash import BashShell
from build import Builder
from largefile import LargeFile
from rsync import RsyncSession
from splitter import Splitter
from verify import Verifier

def _usage():
    print '''\

------------------------------
Usage
  $ python RsyncLargeFiles.py 	[-f <file name or path>] [-d <hostname:/path/chunkdir>]
  				[-b <size of pieces>] [-l <directory for pieces>]
  
Mandatory Options:
  -f				File name or complete path to transfer.
  -d				Remote machine hostname or IP and path to send chunks.
  
Extra Options:
  -b				Split piece size in MB.
  				   Default: Calculated size to create 500 chunks.
  -l				Directory used to store split chunks.
  				   Default: CurrentDirectory/scratch/
				  
  --help			Print this output.
  --filename			Set file to transfer.
  --destination			Remote hostname.
  --size			Chunk size.
  --chunkdir			Directory to store chunks from split command.
  --debug			Produce DEBUG statements.
  --scrub			Removes all temporary chunk files after transfer.'''
  
  
class DefaultOpts(Exception):
    def __init__(self, val):
        self.val = val
    def __str__(self):
        return repr(self.val)
			
  
class Options:
    def __init__(self):
        # Actual script options
        self.largefile = None
        self.file = ''
        self.file_set = None
        self.destination = ''
        self.destination_set = None
        self.remotehostname = ''
        self.remotepath = ''
        self.chunksize = 0
        self.chunksize_set = None
        self.chunkdir = ''
        self.chunkdir_set = None
        self.debug = False
        self.scrub = False
        self.default = []
    
    # Should be in Splitter class,  but is here due to the timing between when
    #    a LargeFile object is instantiated and the Splitter object.
    def calcpiecesize(self):
        self.file = self.largefile.file
        # Estimates chunk size for 500 chunks in MB.
        self.chunksize = round(float(self.largefile.getfilesize()) / 500 / 1024 / 1024, 1)
        return (int(self.chunksize) + 1)
    
    # Set option class attributes based on those supplied from the user.
    def parseoptions(self):
        optns = 'f:d:b:l:'
        keywords = ['help', 'file=', 'destination=', 'size=', 'chunkdir=', 'debug', 'scrub']
        try:
            opts,  extraparams = getopt.getopt(sys.argv[1:],  optns,  keywords)
            #print 'Opts: ',  opts
            #print 'extraparams: ',  extraparams
            for o, p in opts:
                if o in ['-h', '--help']:
                    _usage()
                    sys.exit(0)
                if o in ['-f', '--file']:
                    self.file = p
                    self.file_set = True
                if o in ['-d', '--destination']:
                    self.destination = p
                    self.destination_set = True
                if o in ['-b', '--size']:
                    self.chunksize = int(p)
                    self.chunksize_set = True
                if o in ['-l', '--chunkdir']:
                    self.chunkdir = p
                    self.chunkdir_set = True
                if o in ['--debug']:
                    self.debug = True
                if o in ['--scrub']:
                    self.scrub = True
            if not self.file_set or not self.destination_set:
                raise getopt.GetoptError('MANDATORY option missing.')
            if not self.chunksize_set or not self.chunkdir_set:
                if not self.chunksize_set:
                    self.default.append('size')
                if not self.chunkdir_set:
                    self.default.append('chunk')
        except getopt.GetoptError,  (strerror):
            if "-f" in str(strerror):
                print "ERROR: '-f' option given but no filename provided."
            elif "-d" in str(strerror):
                print "ERROR: '-d' option given but no destination hostname provided."
            elif "MANDATORY" in str(strerror):
                print "Missing filename or destination hostname option.  Review '-f' and '-d' options."
            else:
                print strerror
            _usage()
            sys.exit(0)
    
    def checkfileexist(self, location):
        # Check if file really exists.
        print "Checking for file: ",  location
        self.largefile.fileexists(location)
        if not self.largefile.exists == 1:
            print "File exists: (ERROR) No.  Check path and filename."
            print "Exiting . . ."
            sys.exit(0)
        else:
            print 'File exists: Yes!'
                    
    def sizeflag(self):
        if "size" in self.default:
            # Truncate the value and add 1.
            self.chunksize = self.calcpiecesize()
            print "Chunk size: '-b' chunk size not specified.  Using default.  Creating ~500 chunks @ (", self.chunksize, "MB)."
            
    def chunkdirflag(self):
        # Was the option set,  or are we using the default?
        if "chunk" in self.default:
            self.chunkdir = str(os.getcwd())+'/chunks/'
            print "Chunk directory: '-l' chunk directory not specified.  Using default 'chunks' directory in current working directory."
        else:
            print "Chunk directory: ",  self.chunkdir
                
        # Check if the location exists
        self.largefile.fileexists(self.chunkdir)
        if not self.largefile.exists == 1:
            print "Chunk directory: (ERROR) No (", self.chunkdir, ") directory exists."
            create = raw_input("Would you like to create it? [y/n]: ")
            if create == 'y' or create == 'yes':
                os.mkdir(self.chunkdir)
            else:
                sys.exit(0)
        else:
            print 'Chunk directory exists: Yes!'
                
        # Check if we can read/write to it
        if not writeable(self.chunkdir):
            print "Permissions: (ERROR) Current user does not have access to specified directory."
            print "Exiting."
            sys.exit(0)
        else:
            print "Permissions: Good!"

    def splithostname(self):
        self.remotehost, self.remotepath = self.destination.split(":", 1)
    
    # Checks user-supplied options.  Do the files/directories exist?  Can we write to them?
    def checkoptions(self):
        self.checkfileexist(self.file)
        self.sizeflag()
        self.chunkdirflag()
        self.splithostname()

        if self.debug:
            self.debugmode()

    def debugmode(self):
        print '''\
-----------------------
DEBUG INFORMATION
Options used:
-f:   %s
-d:   %s | host: %s,  remote path: %s
-l:   %s
-b:   %s
--debug used?: %s
--scrub used?: %s
-----------------------''' % (self.file, self.destination, self.remotehost,
                              self.remotepath, self.chunkdir, self.chunksize,
                              self.debug, self.scrub)
			
                
def writeable(chunkdir):
    st = os.stat(chunkdir)
    uid = st.st_uid
    gid = st.st_gid
    
    # Owner of the directory
    user = pwd.getpwuid(uid)[0]
    # Current user.
    c_user = pwd.getpwuid(os.getuid())[0]
    
    group = grp.getgrgid(gid)[0]
    c_group = grp.getgrgid(os.getgid())[0]
    
    # If the current user owns the destination path,  check write access.
    if user == c_user:
        return bool(st.st_mode & stat.S_IWUSR)
    # Check if user is part of group who owns directory.
    elif group == c_group:
        return bool(st.st_mode & stat.S_IWGRP)
    # Check if there's any hope.
    else:
        return bool(st.st_mode & stat.S_IWOTH)

def gettime():
    current = datetime.now()
    return current
	
def main():

    # Create class objects
    shell = BashShell()
    options = Options()
    options.parseoptions()
    
    # Create largefile object with filename from getArgs()
    largefile = LargeFile(options, shell)
    options.largefile = largefile
    
    # Check if full path is given or just local filename.  Build full path.
    if "/" in str(options.file):
        largefile.basename = largefile.getbasename()
    else:
        largefile.basename = options.file
        options.file = largefile.fetchpath()+"/"+options.file
            
    # Get chunk directory full path.
    options.chunkdir = largefile.fetchpath()+"/"+options.chunkdir
            
    options.checkoptions()
            
    # Create splitter object with filename,  size,  and chunkdir/size from getArgs/calcpiecesize.
    splitter = Splitter(options, shell, largefile)
    session = RsyncSession(options, shell, largefile, splitter)
    sys.stdout.write("------------\n")
    if splitter.precheck(session):
        shell.start = gettime()
        splitter.split()
        print "\n>>>> Waiting for split to finish..."
        while splitter.sShell.getqueue() == 1:
            pass
        shell.end = gettime()
        shell.getruntime('Splitting')

        sys.stdout.write("\n")

        # Initiate rsync sessions.
        sys.stdout.write("------------\n")
        shell.start = gettime()
    else:
        print 'Skip split.  Begin transfer.'

    shell.process = 'rsync'
    session.updateprogress()
    shell.start = gettime()
    if session.getlocalcount() != session.getremotecount() and not session.checkfile():
        # Single rsync session to handle all chunks with <filename>_a* <filename>_b* etc....
        for letter in ascii_lowercase:
            session.fileset = letter
            # Starts initial set of rsync sessions.
            if shell.getqueue() < 5:
                session.callrsync()
                session.updateprogress()
                time.sleep(2)
            while shell.getqueue() == 5:
                session.updateprogress()
        while session.getremotecount() != session.getlocalcount():
            if shell.getqueue() < 1:
                session.fileset = '*'
                session.callrsync()
            session.updateprogress()
            time.sleep(2)

        session.updateprogress()
            
    shell.end = gettime()
    shell.getruntime('Rsyncing')

    sys.stdout.write("\n")
    
    sys.stdout.write("------------\n")
    verifier = Verifier(shell, session)
    shell.start = gettime()
    verifier.comparefiles()
    shell.end = gettime()
    shell.getruntime('Verification')

    sys.stdout.write("\n")

    if not session.checkfile():
        # Cat the file back together.
        builder = Builder(shell, session, largefile)
        sys.stdout.write("------------\n")
        shell.start = gettime()
        builder.cat()
        while builder.buildershell.progress != 100.0:
            builder.progress()
            time.sleep(2)
        # md5sum to make sure it's a legitimate transfer.
        sys.stdout.write("\nRunning a checksum to verify remote file integrity...\n")
        builder.comparesums()
        shell.end = gettime()
        shell.getruntime('Building')
    
    # If clean option is set.
    if options.scrub:
        builder.clean()
    else:
        print "Make sure to remove the temporary chunks on both the local and remote servers."
                               

if __name__ == '__main__':
    try:
        main()
        print '''
Done.'''
    except KeyboardInterrupt:
        print "{!! Aborting !!}"
        sys.exit(0)
