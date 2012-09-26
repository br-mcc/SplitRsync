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
  $ python RsyncLargeFiles.py 	[-f <file name or path>] [-d <remote machine>] [-b <size of pieces>]
  				[-l <directory for pieces>]
  
Mandatory Options:
  -f				File name or complete path to transfer.
  -d				Remote machine hostname or IP.
  
Extra Options:
  -b				Split piece size in MB.
  				   Default: Calculated size to create 400 chunks.
  -l				Directory used to store split chunks.
  				   Default: /scratch/
				  
  --help			Print this output.
  --filename			Set file to transfer.
  --destination			Remote hostname.
  --size			Chunk size.
  --chunkdir			Directory to store chunks from split command.'''
  
  
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

                # Performance variables
                self.RUNBASH = 0
                self.PRINTPROGRESS = 0
                self.CONVERTDATETIME = 0
                self.GETRUNTIME = 0
		
	def runBash(self):
                self.RUNBASH += 1
                if self.flag == 0:
                        subprocess.call(self.cmd, shell=True)
		elif self.flag == 1:
			p = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE)
			out = p.communicate()[0]
			return out

	def printProgress(self,prompt):
                self.PRINTPROGRESS += 1
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
                self.CONVERTDATETIME += 1
		return time.seconds//3600,(time.seconds//60)%60,time.seconds%60

	def getRunTime(self,action):
                self.GETRUNTIME += 1
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
		self.hostname = ''
		self.hostname_set = None
		self.chunksize = 0
		self.chunksize_set = None
		self.chunkdir = ''
		self.chunkdir_set = None
		self.debug = False
		self.scrub = False
		self.default = []

                # Performance variables
		self.CALCPIECESIZE = 0
		self.PARSEOPTIONS = 0
		self.CHECKOPTIONS = 0
		self.DEBUGMODE = 0
	
	# Should be in Splitter class, but is here due to the timing between when
	#    a LargeFile object is instantiated and the Splitter object.
	def calcPieceSize(self,largefile):
                self.CALCPIECESIZE += 1
		self.file = largefile.file
		# Estimates chunk size for 500 chunks in MB.
		self.chunksize = round(float(largefile.getFileSize()) / 500 / 1024 / 1024,1)
		return (int(self.chunksize) + 1)
	
	# Set option class attributes based on those supplied from the user.
	def parseOptions(self):
                self.PARSEOPTIONS += 1
		optns = 'f:d:b:l:'
		keywords = ['help','file=','destination=','size=','chunkdir=','debug','scrub']
		try:
			opts, extraparams = getopt.getopt(sys.argv[1:], optns, keywords)
			#print 'Opts: ', opts
			#print 'extraparams: ', extraparams
			for o,p in opts:
				if o in ['-h','--help']:
					_usage()
					sys.exit(0)
				if o in ['-f','--file']:
					self.file = p
					self.file_set = True
				if o in ['-d','--destination']:
					self.hostname = p
					self.hostname_set = True
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
	
	# Checks user-supplied options.  Do the files/directories exist?  Can we write to them?
	def checkOptions(self,largefile):
                self.CHECKOPTIONS += 1
		# Check if file really exists.
		print "Checking for file: ", self.file
		largefile.fileExists(self.file)
		if not largefile.exists == 1:
			print "File exists: (ERROR) No.  Check path and filename."
			print "Exiting . . ."
			sys.exit(0)
		else:
			print 'File exists: Yes!'
		
		# Set defaults for missing options.
		# Calculate and set chunk size.
		if "size" in self.default:
			# Truncate the value and add 1.
			self.chunksize = self.calcPieceSize(largefile)
			print "Chunk size: '-b' chunk size not specified.  Using default.  Creating ~500 chunks @ (",self.chunksize,"MB)."
		# Set default chunk directory.
		if "chunk" in self.default:
			self.chunkdir = str(os.getcwd())
			print "Chunk directory: '-l' chunk directory not specified.  Using default '/awips/chps_local/scratch/' directory."
		else:
			print "Chunk directory: ", self.chunkdir
		largefile.fileExists(self.chunkdir)
		if not largefile.exists == 1:
			print "Chunk directory exists: (ERROR) No (",self.chunkdir,") does not exist."
			print "Exiting . . ."
			sys.exit(0)
		else:
			print 'Chunk directory exists: Yes!'
			
		# Check if we can create our chunks in the chunk directory.
		if not isWriteable(self.chunkdir):
			print "Permissions: (ERROR) Current user does not have access to specified directory."
			print "Exiting."
			sys.exit(0)
		else:
			print "Permissions: Good!"

		if self.debug:
                        self.debugmode()

	def debugMode(self):
                self.DEBUGMODE += 1
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

		# Performance variables
		self.FILEEXISTS = 0
		self.GETBASENAME = 0
		self.GETFILESIZE = 0
		self.GETLOCALSUM = 0
	
	def fileExists(self,filedir):
                self.FILEEXISTS += 1
		filedir = filedir
		self.shell.cmd = 'ls -ld '+filedir+'|wc -l'
		self.shell.flag = 1
		self.exists = int(self.shell.runBash())
	
	def getBaseName(self):
                self.GETBASENAME += 1
		print "File: ", self.file
		return self.file.rsplit('/',1)[1]

	def getFileSize(self):
                self.GETFILESIZE += 1
		filestat = os.stat(self.file)
		self.size = filestat.st_size
		return self.size

	def fetchPath(self):
                self.FETCHPATH += 1
		self.shell.cmd = 'pwd'
		self.shell.flag = 1
		path = self.shell.runBash()
		# Return path as string stripped of special characters.
		return str(path).strip()

	def getLocalSum(self):
                self.GETLOCALSUM += 1
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
		self.cmd = ''
		self.numPieces = 0

		# Performance variables
		self.CALCPIECES = 0
		self.SPLIT = 0
			
	def calcPieces(self):
                self.CALCPIECES += 1
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
                self.SPLIT += 1
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
                self.remote = self.options.hostname
                self.chunkdir = self.options.chunkdir
                self.totalPieces = self.getLocalCount()
                self.numPieces = 0
                self.fileset = ''
                self.progress = 0
                self.synch_queue = 0

                # Performance variables
                self.CALLRSYNC = 0
                self.GETQUEUE = 0
                self.GETLOCALCOUNT = 0
                self.GETREMOTECOUNT = 0
                self.UPDATEPROGRESS = 0
                self.VERIFYINTEGRITY = 0

        def callRsync(self):
                self.CALLRSYNC += 1
                ''' Build Rsync command and create rsynch process.'''
                source = self.file+'_'+self.fileset+'*'
                destination = self.remote+':'+self.chunkdir+'/'
                self.syncshell.cmd = 'rsync -rlz --include "'+source+'" --exclude "*" '+self.chunkdir+' '+destination+' 2> /dev/null &'
                self.syncshell.flag = 0
                #self.shell.pid_catch = 1
                #self.pid = self.shell.runBash()
                #self.synch_queue.append(self.pid)
                self.syncshell.runBash()

        def getQueue(self):
                self.SETQUEUE += 1
                ''' Track active Rsynch processes.'''
                self.syncshell.cmd = 'ps -eaf|grep "rsync -rlz"|grep -v grep|wc -l'
                self.syncshell.flag = 1
                self.synch_queue = int(self.syncshell.runBash())
		return int(self.synch_queue)

	def getLocalCount(self):
                self.GETLOCALCOUNT += 1
                self.syncshell.cmd = 'ls -l '+self.chunkdir+'/|wc -l'
                self.syncshell.flag = 1
                count = self.syncshell.runBash()
                return int(count)

        def getRemoteCount(self):
                self.GETREMOTECOUNT += 1
                ''' Check remote system for completed file transfers.'''
                self.syncshell.cmd = "ssh -q "+self.remote+" 'ls -l "+self.chunkdir+"|wc -l'"
                self.syncshell.flag = 1
                chunksDone = self.syncshell.runBash()
                return int(chunksDone)

        def updateProgress(self):
                self.UPDATEPROGRESS += 1
                self.syncshell.current = self.getRemoteCount()
                self.syncshell.total = self.getLocalCount()
                self.syncshell.printProgress('Transferring: ')

        def verifyIntegrity(self):
                self.VERIFYINTEGRITY += 1
		self.syncshell.current = 0
		self.syncshell.total = self.getLocalCount()
                for f in glob.glob(self.chunkdir+'*_*'):
			self.syncshell.printProgress('Verifying remote files: ')
			self.syncshell.current += 1
			self.syncshell.cmd = "ssh -q "+self.remote+" 'ls -l "+f+"'|awk '{print $5}'"
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

                # Performance variables
                self.CAT = 0
                self.GETREMOTESIZE = 0
                self.PROGRESS = 0
                self.COMPARESUMS = 0
                
        def cat(self):
                self.CAT += 1
                self.buildershell.cmd = "ssh "+self.session.remote+" 'cd "+self.session.chunkdir+"; cat "+self.session.file+"_* > "+self.session.file+"  &'"
                self.buildershell.flag = 0
                self.buildershell.runBash()
		self.buildershell.progress=0

	def getRemoteSize(self):
                self.GETREMOTESIZE += 1
                self.buildershell.cmd = "ssh -q "+self.session.remote+" 'ls -l "+self.session.chunkdir+self.session.file+"'|awk '{print $5}'"
                self.buildershell.flag = 1
                remotefilesize = self.buildershell.runBash()
                return int(remotefilesize)

        def progress(self):
                self.PROGRESS += 1
                self.buildershell.current = self.getRemoteSize()
                self.buildershell.total = self.localfilesize
                self.buildershell.printProgress('Building: ')

        def compareSums(self):
                self.COMPARESUMS += 1
                self.buildershell.cmd = "ssh -q "+self.session.remote+" 'md5sum "+self.session.chunkdir+self.session.file+"'|awk '{print $1}'"
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
	shell.cmd = "ssh -q "+session.remote+" find "+session.chunkdir+" -name '"+session.file+"_*' -exec rm -f {} \;"
	shell.flag = 0
	shell.runBash()
	shell.cmd = "ssh "+session.remote+" 'ls -l "+session.chunkdir+"'"
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
        shell.getRunTime('Verifying files')

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

        # If debug, show stats
        if options.debug:
                print '''
shell.RUNBASH = %d
shell.PRINTPROGRESS = %d
shell.CONVERTDATETIME = %d
shell.GETRUNTIME = %d
options.CALCPIECESIZE = %d
options.PARSEOPTIONS = %d
options.CHECKOPTIONS = %d
options.DEBUGMODE = %d
largefile.FILEEXISTS = %d
largefile.GETBASENAME = %d
largefile.GETFILESIZE = %d
largefile.GETLOCALSUM = %d
splitter.CALCPIECES = %d
splitter.SPLIT = %d
session.CALLRSYNC = %d
session.GETQUEUE = %d
session.GETLOCALCOUNT = %d
session.GETREMOTECOUNT = %d
session.UPDATEPROGRESS = %d
session.VERIFYINTEGRITY = %d
builder.CAT = %d
builder.GETREMOTESIZE = %d
builder.PROGRESS = %d
builder.COMPARESUMS = %d''' % (shell.RUNBASH,shell.PRINTPROGRESS,shell.CONVERTDATETIME,shell.GETRUNTIME,\
                               shell.CALCPIECESIZE,options.PARSEOPTIONS,options.CHECKOPTIONS,options.DEBUGMODE,\
                               largefile.FILEEXISTS,largefile.GETBASENAME,largefile.GETFILESIZE,\
                               largefile.GETLOCALSUM,splitter.CALCPIECES,splitter.SPLIT,session.CALLRSYNC,\
                               session.GETQUEUE,session.GETLOCALCOUNT,session.GETREMOTECOUNT,session.UPDATEPROGRESS,\
                               session.VERIFYINTEGRITY,builder.CAT,builder.GETREMOTESIZE,builder.PROGRESS,\
                               builder.COMPARESUMS)
                               
                            
                

if __name__ == '__main__':
	try:
		main()
		print '''
Done.'''
	except KeyboardInterrupt:
		print "{!! Aborting !!}"
		sys.exit(0)
