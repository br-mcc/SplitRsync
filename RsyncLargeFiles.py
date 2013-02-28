#!/bin/python

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
import subprocess
# Threading for performance/management
import threading

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
		
 
class BashShell:
    def __init__(self):
        self.cmd = ''
        self.flag = ''
        self.pid_catch = 0
        self.current = 0
        self.total = 0
        self.start = None
        self.end = None
        self.process = ''
            
    def runbash(self):
        if self.flag == 0:
            subprocess.call(self.cmd,  shell=True)
        elif self.flag == 1:
            p = subprocess.Popen(self.cmd,  shell=True,  stdout=subprocess.PIPE)
            out = p.communicate()[0]
            return out

    def getqueue(self):
        ''' Track active processes.'''
        if self.process == 'rsync':
            check = 'rsync -rlz'
        elif self.process == 'split':
            check = 'split -b'
        self.cmd = 'ps -eaf|grep "%s"|grep -v grep|wc -l' % (check)
        self.flag = 1
        return int(self.runbash())

    def printprogress(self, prompt):
        try:
            self.progress = round(float(self.current) / self.total * 100)
        except ZeroDivisionError:
            self.progress = 0.
        except ValueError:
            print "Current: ", self.current
            print "Total: ", self.total
            print "Progress: ", self.progress
            sys.exit(0)
        percentmessage = "\r" + prompt + " " + str(self.progress)+"%"
        progressmessage = "[" +str(self.current).strip()+ "/" +str(self.total).strip()+ "]"
        sys.stdout.write(percentmessage + " || " + progressmessage)
        sys.stdout.flush()
        time.sleep(0.5)
            
    def convertdatetime(self, time):
        return time.seconds//3600, (time.seconds//60)%60, time.seconds%60

    def getruntime(self, action):
        runtime = self.end - self.start
        hours, minutes, seconds = self.convertdatetime(runtime)
        print '''
%s took %s hours %s minutes and %s seconds
------------\n''' % (action, hours, minutes, seconds)
			
  
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
            print ("Chunk size: '-b' chunk size not specified.  Using default."
                   "  Creating ~500 chunks @ (", self.chunksize, "MB).")
            
    def chunkdirflag(self):
        # Was the option set,  or are we using the default?
        if "chunk" in self.default:
            self.chunkdir = str(os.getcwd())+'/chunks/'
            print ("Chunk directory: '-l' chunk directory not specified."
                   "  Using default 'chunks' directory in current working directory.")
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
        # Warn user if number of chunks exceeds number of chunk
        #   suffix combinations ("file.tar.gz_zz")
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
        self.rShell.cmd = "ssh -qq %s 'ls -l %s/%s'|wc -l 2> /dev/null" % \
                          (self.host, self.hostpath, self.file)
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
        self.rShell.cmd = 'rsync -rlz --include "%s" --exclude "*" %s/ %s/ 2> /dev/null &' % \
                          (source, self.chunkdir, self.options.destination)
        self.rShell.flag = 0
        self.rShell.runbash()

    def getlocalcount(self):
        self.rShell.cmd = 'ls -l %s/%s_*|wc -l 2> /dev/null' % (self.chunkdir, self.file)
        self.rShell.flag = 1
        count = int(self.rShell.runbash())
        return count

    def getremotecount(self):
        ''' Check remote system for completed file transfers.'''
        self.rShell.cmd = "ssh -qq %s 'ls -l %s/%s_*|wc -l 2> /dev/null'" % \
                          (self.host, self.hostpath, self.file)
        self.rShell.flag = 1
        count = int(self.rShell.runbash())
        return count

    def updateprogress(self):
        self.rShell.current = self.getremotecount()
        self.rShell.total = self.getlocalcount()
        self.rShell.printprogress('Transferring: ')

    def clean():
        rShell.cmd = "ssh -qq %s find %s -name '%s_*' -exec rm -f {} \;" % \
                     (self.host, self.hostpath, self.file)
        rShell.flag = 0
        rShell.runbash()
        rShell.cmd = "ssh -qq %s 'ls -l %s'" % (self.host, self.hostpath)
        rShell.flag = 1
        contents = rShell.runbash()
        print '''
    Remote directory contents after cleaning...
    %s''' % contents


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
            self.vShell.cmd = "cd %s; ls  -l %s_%s*|awk '{print $5, $NF}'" % \
                              (self.vSession.chunkdir, self.vSession.file, self.set)
        else:
            self.vShell.cmd = "ssh %s 'cd %s; ls -l %s_%s*'|awk '{print $5, $NF}'" % \
                              (self.vSession.host, self.vSession.hostpath, self.vSession.file, self.set)
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
        
					
class Builder:
    def __init__(self, shell, session, largefile):
        self.buildershell = shell
        self.session = session
        self.localfilesize = largefile.size
        self.localsum = largefile.checksum
        self.remotesum = ''
            
    def cat(self):
        self.buildershell.cmd = "ssh %s 'cd %s; cat %s_* > %s  &'" % \
                                (self.session.host, self.session.hostpath, \
                                 self.session.file, self.session.file)
        self.buildershell.flag = 0
        self.buildershell.runbash()
        self.buildershell.progress=0

    def getremotesize(self):
        self.buildershell.cmd = "ssh -qq %s 'ls -l %s/%s'|awk '{print $5}'" % \
                                (self.session.host,  self.session.hostpath, self.session.file)
        self.buildershell.flag = 1
        remotefilesize = self.buildershell.runbash()
        return int(remotefilesize)

    def progress(self):
        self.buildershell.current = self.getremotesize()
        self.buildershell.total = self.localfilesize
        self.buildershell.printprogress('Building: ')

    def comparesums(self):
        self.buildershell.cmd = "ssh -qq %s 'md5sum %s/%s'|awk '{print $1}'" % \
                                (self.session.host, self.session.hostpath, self.session.file)
        self.buildershell.flag = 1
        self.remotesum = self.buildershell.runbash()
        print '''
Local sum:  %s
Remote sum: %s''' % (self.localsum,  self.remotesum)
        if str(self.remotesum).strip() != str(self.localsum).strip():
            print ('ERROR:  Checksums don\'t match!  There might'
                   ' have been a problem during the file transfer!')
            sys.exit(0)
        else:
            print 'Transfer and rebuild of file succeeded!'
			
                
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
        clean()
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
