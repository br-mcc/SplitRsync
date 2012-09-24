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
		self.linecount = 0
		self.totalcount = 0
		pid = 0
		
	def runBash(self):
                if self.flag == 0:
                        if self.pid_catch == 0:
                                subprocess.call(self.cmd, shell=True)
                        elif self.pid_catch == 1:
                                pid = subprocess.Popen(self.cmd, shell=True).pid
                                return int(pid)
		elif self.flag == 1:
			p = subprocess.Popen(self.cmd, shell=True, stdout=subprocess.PIPE)
			out = p.communicate()[0]
			return out

	def printProgress(self,prompt):
		self.progress = round(float(self.linecount) / self.totalcount * 100)
		sys.stdout.write("\r "+prompt+" " +str(self.progress)+ "%")
		sys.stdout.flush()
		time.sleep(0.5)
			
  
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
		keywords = ['help','file=','destination=','size=','chunkdir=','debug']
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
                print '''\
-----------------------
DEBUG INFORMATION
Options used:
   -f:   %s
   -d:   %s
   -l:   %s
   -b:   %s
-----------------------''' % (self.file,self.hostname,self.chunkdir,self.chunksize)
	
		
class RemoteHost:
	def __init__(self,options,largefile):
		self.options = options
		self.file = largefile.file
		self.hostname = self.options.hostname
		self.chunkdir = self.options.chunkdir
		self.numfiles = 0
		
class Splitter:
	def __init__(self,options,shell,largefile):
		self.options = options
		self.shell = shell
		self.largefile = largefile
		self.file = largefile.file
		self.basename = largefile.basename
		self.filesize =  largefile.size
		self.chunksize = self.options.chunksize
		self.chunkdir = self.options.chunkdir
		self.cmd = ''
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
		print self.path
		
		self.shell.cmd = 'split -b '+str(self.chunksize)+'m ' +self.file+ ' ' +self.path+ '_ &'
		self.shell.flag = 0
		self.shell.runBash()

		# Print progress of split command.
		self.count = 0
		self.shell.cmd = 'ls -l ' +self.path+ '* |wc -l'
		self.shell.flag = 1
		self.shell.totalcount = self.numPieces
		while self.shell.linecount < self.numPieces:
			self.shell.linecount = int(self.shell.runBash())
			self.shell.printProgress('Splitting: ')
	
		
class LargeFile:
	def __init__(self,options,shell):
		self.file = options.file
		self.basename = ''
		self.shell = shell
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


class RsyncSession:
        def __init__(self,options,shell,largefile,splitter):
                self.options = options
                self.shell = shell
                self.largefile = largefile
                self.splitter = splitter
                self.file = self.largefile.file
                self.remote = self.options.hostname
                self.chunkdir = self.options.chunkdir
                self.totalPieces = self.splitter.numPieces
                self.numPieces = 0
                self.fileset = ''
                self.progress = 0
                self.synch_queue = 0
                self.alphabet = ['abcdefghijklmnopqrstuvwxyz']

        def callRsync(self):
                ''' Build Rsync command and create rsynch process.'''
                source = self.file+'_'+self.fileset+'*'
                destination = self.remote+':'+self.chunkdir+'/.'
                self.shell.cmd = 'rsync -rlz --include "'+source+'" --exclude "*" '+self.chunkdir+' '+destination+' &'
                self.shell.flag = 0
                #self.shell.pid_catch = 1
                #self.pid = self.shell.runBash()
                #self.synch_queue.append(self.pid)
                self.shell.runBash()

        def trackActive(self):
                ''' Track active Rsynch processes.'''
                self.shell.cmd = 'ps -eaf|grep "rsync -rlz"|grep -v grep|wc -l'
                self.shell.flag = 1
                self.synch_queue = int(self.shell.runBash())
                if self.synch_queue < 8:
                        self.callRsync()

        def checkRemote(self):
                ''' Check remote system for completed file transfers.'''
                self.shell.cmd = "ssh "+self.remote+" 'ls -l "+self.chunkdir+"|wc -l'"
                self.shell.flag = 1
                chunksDone = self.shell.runBash()
                self.shell.linecount = chunksDone
                self.shell.totalcount = self.totalPieces
                self.shell.printProgress('Transferring: ')
                        
			

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
	splitter.split()

        # Initiate rsync sessions.
        session = RsyncSession(options,shell,largefile,splitter)
        for letter in ascii_lowercase:
                session.fileset = letter
                session.trackActive()
                while session.synch_queue == 8:
                        session.trackActive()
                        session.checkRemote()

        while shell.progresse < 100:
                session.checkRemote()
	

if __name__ == '__main__':
	try:
		main()
		print '''
Done.'''
	except KeyboardInterrupt:
		print "{!! Aborting !!}"
		sys.exit(0)
