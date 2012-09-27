#!/usr/bin/python

import sys
from string import ascii_lowercase
# Used for checking user/group permissions
import os
import pwd
import grp
import stat
# Used for issuing bash commands and collecting script options.
import time
import getopt
import subprocess
# Checksums
import glob
# Calculate runtime
from datetime import datetime, timedelta

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
  --clean			Removes all temporary chunk files after transfer.'''
  
  
class DefaultOpts(Exception):
	def __init__(self,val):
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
		
	def runBash(self):
                if self.flag == 0:
                        subprocess.call(self.cmd, shell=True)
		elif self.flag == 1:
			p = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE)
			out = p.communicate()[0]
			return out

	def printProgress(self,prompt):
		try:
			self.progress = round(float(self.current) / self.total * 100)
		except ZeroDivisionError:
			self.progress = 0.
		except ValueError:
                        print "Current: ",self.current
                        print "Total: ",self.total
                        print "Progress: ",self.progress
                        sys.exit(0)
		sys.stdout.write("\r"+prompt+" " +str(self.progress)+ "% || ["+str(self.current).strip()+"/"+str(self.total).strip()+"]")
		sys.stdout.flush()
		time.sleep(0.5)
		
	def convertDateTime(self,time):
		return time.seconds//3600,(time.seconds//60)%60,time.seconds%60

	def getRunTime(self,action):
                runtime = self.end - self.start
		hours,minutes,seconds = self.convertDateTime(runtime)
                print '''
%s took %s hours %s minutes and %s seconds
------------\n''' % (action,hours,minutes,seconds)
			
  
class Options:
	def __init__(self):
		# Actual script options
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
	
	# Should be in Splitter class, but is here due to the timing between when
	#    a LargeFile object is instantiated and the Splitter object.
	def calcPieceSize(self,largefile):
		self.file = largefile.file
		# Estimates chunk size for 500 chunks in MB.
		self.chunksize = round(float(largefile.getFileSize()) / 500 / 1024 / 1024,1)
		return (int(self.chunksize) + 1)
	
	# Set option class attributes based on those supplied from the user.
	def parseOptions(self):
		optns = 'f:d:b:l:'
		keywords = ['help','file=','destination=','size=','chunkdir=','debug','scrub']
		try:
			opts, extraparams = getopt.getopt(sys.argv[1:], optns, keywords)
			#print 'Opts: ', opts
			#print 'extraparams: ', extraparams
			for o,p in opts:
				if o in ['-h','--help']:
					_usage()
					sys.eit(0)
				if o in ['-f','--file']:
					self.file = p
					self.file_set = True
				if o in ['-d','--destination']:
					self.destination = p
					self.destination = True
				if o in ['-b','--size']:
					self.chunksize = int(p)
					self.chunksize_set = True
				if o in ['-l','--chunkdir']:
					self.chunkdir = p
					self.chunkdir_set = True
				if o in ['--debug']:
                                        self.debug = True
				if o in ['--scrub']:
					self.scrub = True
			if not self.file_set or not self.hostname_set:
				raise getopt.GetoptError('MANDATORY option missing.')
			if not self.chunksize_set or not self.chunkdir_set:
				if not self.chunksize_set:
					self.default.append('size')
				if not self.chunkdir_set:
					self.default.append('chunk')
		except getopt.GetoptError, (strerror):
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
	
	def checkFileExist(self):
		# Check if file really exists.
		print "Checking for file: ", self.file
		largefile.fileExists(self.file)
		if not largefile.exists == 1:
			print "File exists: (ERROR) No.  Check path and filename."
			print "Exiting . . ."
			sys.exit(0)
		else:
			print 'File exists: Yes!'
			
	def sizeFlag(self):
		if "size" in self.default:
			# Truncate the value and add 1.
			self.chunksize = self.calcPieceSize(largefile)
			print "Chunk size: '-b' chunk size not specified.  Using default.  Creating ~500 chunks @ (",self.chunksize,"MB)."
			
	def chunkdirFlag(self):
		if "chunk" in self.default:
			self.chunkdir = str(os.getcwd())+'/chunks/'
			print "Chunk directory: '-l' chunk directory not specified.  Using default 'chunks' directory in current working directory."
		else:
			print "Chunk directory: ", self.chunkdir
			
	def checkChunkDirExist(self):
		largefile.fileExists(self.chunkdir)
		if not largefile.exists == 1:
			print "Chunk directory: (ERROR) No (",self.chunkdir,") directory exists."
			create = raw_input("Would you like to create it? [y/n]: ")
			if create == 'y' or create == 'yes':
				os.mkdir(self.chunkdir)
			else:
				sys.exit(0)
		else:
			print 'Chunk directory exists: Yes!'
			
	def checkChunkDirWriteable(self):
		if not isWriteable(self.chunkdir):
			print "Permissions: (ERROR) Current user does not have access to specified directory."
			print "Exiting."
			sys.exit(0)
		else:
			print "Permissions: Good!"

	def splitHostname():
		self.remotehost,self.remotepath = self.destination.lsplit(":",1)
		print self.remotehost, self.remotepath
	
	# Checks user-supplied options.  Do the files/directories exist?  Can we write to them?
	def checkOptions(self,largefile):
		
		self.checkFileExist()
		self.sizeFlag()
		self.chunkdirFlag()
		self.checkChunkDirExist()
		self.checkChunkDirWriteable()
		self.splitHostname()

		if self.debug:
                        self.debugMode()

	def debugMode(self):
                print '''\
-----------------------
DEBUG INFORMATION
Options used:
   -f:   %s
   -d:   %s
   -l:   %s
   -b:   %s
   --debug used?: %s
   --scrub used?: %s
-----------------------''' % (self.file,self.hostname,self.chunkdir,self.chunksize,self.debug,self.scrub)


class LargeFile:
	def __init__(self,options,shell):
		self.file = options.file
		self.shell = shell
		self.checksum = self.getLocalSum()
		self.basename = ''
		self.size = self.getFileSize()
		self.progress = 0
		self.exists = 0
	
	def fileExists(self,filedir):
		filedir = filedir
		self.shell.cmd = 'ls -ld '+filedir+'|wc -l'
		self.shell.flag = 1
		self.exists = int(self.shell.runBash())
	
	def getBaseName(self):
		print "File: ", self.file
		return self.file.rsplit('/',1)[1]

	def getFileSize(self):
		filestat = os.stat(self.file)
		self.size = filestat.st_size
		return self.size

	def fetchPath(self):
		self.shell.cmd = 'pwd'
		self.shell.flag = 1
		path = self.shell.runBash()
		# Return path as string stripped of special characters.
		return str(path).strip()

	def getLocalSum(self):
		print "\nFetching local file's checksum..."
                self.shell.cmd = "md5sum "+self.file+"|awk '{print $1}'"
                self.shell.flag = 1
                localsum = self.shell.runBash()
                return localsum
		
		
class Splitter:
	def __init__(self,options,shell,largefile):
		self.options = options
		self.splittershell = shell
		self.largefile = largefile
		self.file = largefile.file
		self.basename = largefile.basename
		self.filesize =  largefile.size
		self.chunksize = self.options.chunksize
		self.chunkdir = self.options.chunkdir
		self.numPieces = 0
			
	def calcPieces(self):
		# Given a non-default size option, calculate number of chunks.
		self.numPieces = round(float(self.filesize) / (self.chunksize * 1024 * 1024)) 
		print ">>>>> Estimated number of chunks of size",self.chunksize,"MB: ", self.numPieces 
		# Too many pieces will be created.  Warn user and exit.
		if self.numPieces > 676:
			self.chunksize = self.options.calcPieceSize(self.largefile)
			print "Error: Option '-b' too small.  Too many chunks will be created."
			print "       >>>>>> Try a value of (x) where: ",self.chunksize," < x < 1024"
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
		self.calcPieces()
		
		self.basename = self.largefile.basename
		self.path = self.chunkdir+'/'+str(self.basename)
		
		self.splittershell.cmd = 'split -b '+str(self.chunksize)+'m ' +self.file+ ' ' +self.path+ '_ &'
		self.splittershell.flag = 0
		self.splittershell.runBash()

		# Print progress of split command.
		self.count = 0
		self.splittershell.cmd = 'ls -l ' +self.path+ '* |wc -l'
		self.splittershell.flag = 1
		self.splittershell.total = self.numPieces
		while self.splittershell.current < self.numPieces:
			self.splittershell.current = int(self.splittershell.runBash())
			self.splittershell.printProgress('Splitting: ')


class RsyncSession:
        def __init__(self,options,shell,largefile,splitter):
                self.options = options
                self.syncshell = shell
                self.largefile = largefile
                self.splitter = splitter
                self.file = self.largefile.basename
                self.host = self.options.remotehost
		self.hostpath = self.options.remotepath
                self.chunkdir = self.options.chunkdir
                self.totalPieces = self.getLocalCount()
                self.numPieces = 0
                self.fileset = ''
                self.progress = 0
                self.synch_queue = 0

        def callRsync(self):
                ''' Build Rsync command and create rsynch process.'''
                source = self.file+'_'+self.fileset+'*'
                self.syncshell.cmd = 'rsync -rlz --include "'+source+'" --exclude "*" '+self.chunkdir+' '+self.options.destination+' 2> /dev/null &'
                self.syncshell.flag = 0
                #self.shell.pid_catch = 1
                #self.pid = self.shell.runBash()
                #self.synch_queue.append(self.pid)
                self.syncshell.runBash()

        def getQueue(self):
                ''' Track active Rsynch processes.'''
                self.syncshell.cmd = 'ps -eaf|grep "rsync -rlz"|grep -v grep|wc -l'
                self.syncshell.flag = 1
                self.synch_queue = int(self.syncshell.runBash())
		return int(self.synch_queue)

	def getLocalCount(self):
                self.syncshell.cmd = 'ls -l '+self.chunkdir+'/|wc -l'
                self.syncshell.flag = 1
                count = self.syncshell.runBash()
                return int(count)

        def getRemoteCount(self):
                ''' Check remote system for completed file transfers.'''
                self.syncshell.cmd = "ssh -qq "+self.host+" 'ls -l "+self.hostpath+"|wc -l'"
                self.syncshell.flag = 1
		try:
                	chunksDone = int(self.syncshell.runBash())
		except ValueError:
			print self.syncshell.cmd
			print self.syncshell.runBash()
			sys.exit(0)
                return chunksDone

        def updateProgress(self):
                self.syncshell.current = self.getRemoteCount()
                self.syncshell.total = self.getLocalCount()
                self.syncshell.printProgress('Transferring: ')

        def verifyIntegrity(self):
		self.syncshell.current = 0
		self.syncshell.total = self.getLocalCount()
                for f in glob.glob(self.chunkdir+'*_*'):
			self.syncshell.printProgress('Verifying remote files: ')
			self.syncshell.current += 1
			self.syncshell.cmd = "ssh -qq "+self.host+" 'ls -l "+self.hostpath+"/"+f+"'|awk '{print $5}'"
			self.syncshell.flag = 1
			remotesize = int(self.syncshell.runBash())
			self.syncshell.cmd = "ls -l "+f+"|awk '{print $5}'"
			self.syncshell.flag = 1
			try:
				localsize = int(self.syncshell.runBash())
			except ValueError:
				print self.syncshell.cmd
				print localsize
				sys.exit(0)
			if remotesize < localsize:
				self.fileset = f[-2:-1]
				self.callRsync()
				while self.getQueue() == 1:
					self.getQueue()
					time.sleep(2)

					
class Builder:
        def __init__(self,shell,session,largefile):
                self.buildershell = shell
		self.session = session
                self.localfilesize = largefile.size
                self.localsum = largefile.checksum
                self.remotesum = ''
                
        def cat(self):
                self.buildershell.cmd = "ssh "+self.session.host+" 'cd "+self.session.hostpath+"; cat "+self.session.file+"_* > "+self.session.file+"  &'"
                self.buildershell.flag = 0
                self.buildershell.runBash()
		self.buildershell.progress=0

	def getRemoteSize(self):
                self.buildershell.cmd = "ssh -qq "+self.session.host+" 'ls -l "+self.session.hostpath+"/"+self.session.file+"'|awk '{print $5}'"
                self.buildershell.flag = 1
                remotefilesize = self.buildershell.runBash()
                return int(remotefilesize)

        def progress(self):
                self.buildershell.current = self.getRemoteSize()
                self.buildershell.total = self.localfilesize
                self.buildershell.printProgress('Building: ')

        def compareSums(self):
                self.buildershell.cmd = "ssh -qq "+self.session.host+" 'md5sum "+self.session.host+"/"+self.session.file+"'|awk '{print $1}'"
                self.buildershell.flag = 1
                self.remotesum = self.buildershell.runBash()
                print '''
	Local sum:  %s
	Remote sum: %s''' % (self.localsum, self.remotesum)
                if str(self.remotesum).strip() != str(self.localsum).strip():
                        print 'ERROR:  Checksums don\'t match!  There might have been a problem during the file transfer!'
                        sys.exit(0)
                else:
                        print 'Transfer and rebuild of file succeeded!'
			
                
def isWriteable(chunkdir):
	st = os.stat(chunkdir)
	uid = st.st_uid
	gid = st.st_gid
	
	# Owner of the directory
	user = pwd.getpwuid(uid)[0]
	# Current user.
	c_user = pwd.getpwuid(os.getuid())[0]
	
	group = grp.getgrgid(gid)[0]
	c_group = grp.getgrgid(os.getgid())[0]
	
	# If the current user owns the destination path, check write access.
	if user == c_user:
		return bool(st.st_mode & stat.S_IWUSR)
	# Check if user is part of group who owns directory.
	elif group == c_group:
		return bool(st.st_mode & stat.S_IWGRP)
	# Check if there's any hope.
	else:
		return bool(st.st_mode & stat.S_IWOTH)

def getTime():
        time = datetime.now()
        return time

def clean():
	shell.cmd = "ssh -qq "+session.host+" find "+session.hostpath+" -name '"+session.file+"_*' -exec rm -f {} \;"
	shell.flag = 0
	shell.runBash()
	shell.cmd = "ssh -qq "+session.host+" 'ls -l "+session.hostpath+"'"
	shell.flag = 1
	contents = shell.runBash()
	print '''
Remote directory contents after cleaning...
%s''' % contents
	
def main():

        # Create class objects
	shell = BashShell()
	options = Options()
	options.parseOptions()
	
	# Create largefile object with filename from getArgs()
	largefile = LargeFile(options,shell)
	
	# Check if full path is given or just local filename.  Build full path.
	if "/" in str(options.file):
		largefile.basename = largefile.getBaseName()
	else:
		largefile.basename = options.file
		options.file = largefile.fetchPath()+"/"+options.file
		
	# Get chunk directory full path.
	options.chunkdir = largefile.fetchPath()+"/"+options.chunkdir
		
	options.checkOptions(largefile)
		
	# Create splitter object with filename, size, and chunkdir/size from getArgs/calcPieceSize.
	splitter = Splitter(options,shell,largefile)
	sys.stdout.write("------------\n")
	shell.start = getTime()
	splitter.split()
	shell.end = getTime()
	shell.getRunTime('Splitting')

	sys.stdout.write("\n")

        # Initiate rsync sessions.
        session = RsyncSession(options,shell,largefile,splitter)
        sys.stdout.write("------------\n")
        shell.start = getTime()
	session.updateProgress()
        if session.getLocalCount() != session.getRemoteCount():
                # Single rsync session to handle all chunks with <filename>_a* <filename>_b* etc....
                for letter in ascii_lowercase:
                        session.fileset = letter
                        # Starts initial set of rsync sessions.
                        if session.getQueue() < 5:
                                session.callRsync()
				session.updateProgress()
				session.getQueue()
                                time.sleep(2)
                        while session.synch_queue == 5:
                                session.getQueue()
                                session.updateProgress()

                while session.getRemoteCount() != session.getLocalCount():
			session.getQueue()
			session.fileset = '*'
                        if session.getQueue() < 1:
                                session.callRsync()
			session.updateProgress()
 			time.sleep(2)
        shell.end = getTime()
        shell.getRunTime('Rsyncing')

        sys.stdout.write("\n")
        
        sys.stdout.write("------------\n")
        shell.start = getTime()
        session.verifyIntegrity()
        while session.getQueue() == 1:
        	session.getQueue()
        	session.updateProgress()
        	time.sleep(2)
        shell.end = getTime()
        shell.getRunTime('Verification')

        sys.stdout.write("\n")
        
        # Cat the file back together.
        builder = Builder(shell,session,largefile)
        sys.stdout.write("------------\n")
        shell.start = getTime()
        builder.cat()
        while builder.buildershell.progress != 100.0:
                builder.progress()
                time.sleep(2)
        # md5sum to make sure it's a legitimate transfer.
	sys.stdout.write("\nRunning a checksum to verify remote file integrity...\n")
        builder.compareSums()
        shell.end = getTime()
        shell.getRunTime('Building')
	
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
