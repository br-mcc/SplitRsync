#!/usr/bin/env python

import subprocess

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