#############################################################################
# Implementation of File system logic.                        
# AUTHOR	: Gaurav Mukherjee, NYU POLY	
# CREATED DATED	: 12/09/2013
# FILE		: myfuse.py
# PURPOSE	: Handle all file system commands.
# Version	: 1.0                                             
#############################################################################

import errno  	# for error number codes (ENOENT,EACCES)
import fuse	# for binding to the fuse libraries
import stat	# for file properties
import time	# for time access and conversion
import os,sys	# for operating system interfaces 
from myFS import myFS

# for read and write strings as a files.
try:
    from cStringIO import StringIO	
except:
    from StringIO import StringIO

fuse.fuse_python_api = (0, 2)

class MyFS(fuse.Fuse):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)

        # Set some options required by the Python FUSE binding.
        self.flags = 0
	self.fd = 0
        self.multithreaded = 0	
	self.gid = os.getgid()
	self.uid = os.geteuid()
	self.is_dirty = False

	self.myfs = myFS()
	ret = self.myfs.search('/')
	if ret is False:
		self.myfs.open('/')
		print "Created root directory"
		self.myfs.write('/', "Root")
		self.myfs.setinode('/', 1)
		self.myfs.set_id('/', self.gid, self.uid,stat.S_IFDIR | 0755,2)

		t = int(time.time())
		mytime = (t, t, t)
		self.myfs.utime('/', mytime)
		self.is_dirty = False

    def getdir(self, path):
	print "in getdir"

    def getattr(self, path):
	print "getattr function"
	myfs = myFS()
        st = fuse.Stat()
	c  = fuse.FuseGetContext()

       	ret = myfs.search(path)
	print "Already present =", ret 
	if ret is True:
		st.st_ino = int(myfs.getinode(path))
		st.st_uid, st.st_gid = (c['uid'], c['gid'])
		st.st_mode = myfs.getmode(path)
		st.st_nlink = myfs.getlinkcount(path)
		
		if myfs.getlength(path) is not None:
			st.st_size = int(myfs.getlength(path))
		else:
			st.st_size = 0

		tup = myfs.getutime(path)
		st.st_mtime = int(tup[0].strip().split('.')[0])
		st.st_ctime = int(tup[1].strip().split('.')[0])
		st.st_atime = int(tup[2].strip().split('.')[0])

		print "*" * 50 
		print "inode numder = %d" %st.st_ino
		print "*" * 50

		return st
	else:
       		return - errno.ENOENT

    def readdir(self, path, offset):
	print "readdir", path
	yield fuse.Direntry('.')
        yield fuse.Direntry('..')

	myfs = myFS()
	all_files = myfs.ls()
	print "Rest of the files in root dir"

	for e in all_files:
		
		if str(e) == path:
			continue

		if (len(e.split(path))==2):
			  print "%s" %e
			  strpath = e.split(path)
			  strpath = strpath[1]

			  if path == '/':     		
			  	yield fuse.Direntry(str(e[1:]))
			  elif (len(strpath.split('/')) > 2):				
				continue
			  else:
				size=len(path) + 1
				yield fuse.Direntry(str(e[size:]))


    def open(self, path, flags):
	myfs = myFS()
	ret = myfs.search(path)
	if ret is True:
		return 0
	return -errno.ENOENT

    def mkdir(self, path, mode):
	flags = 1
	self.create(path, mode| stat.S_IFDIR,flags)
	return 0

    def rmdir(self, path):
	print "In rmDir"
	myfs = myFS()
	ret = myfs.remove(path)
	return 0

    def create(self, path, mode, flags):
        print "Create Function"
	# cheking file present status
	myfs = myFS()
	ret = self.open(path, flags)

	if ret == -errno.ENOENT:
		# Create the file in database
		ret = myfs.open(path)
		print "Creating the file %s" %path
		current_time = int(time.time())
		new_time = (current_time, current_time, current_time)
		ret = myfs.utime(path, new_time)

		self.fd = len(myfs.ls())
		print "In create:fd = %d" %(self.fd)
		myfs.setinode(path, self.fd)

		st = fuse.Stat()
		if path == '/':
			st.st_nlink = 2
	       		st.st_mode = stat.S_IFDIR | 0755
		
		if flags == 1:
			st.st_nlink = 2
	       		st.st_mode = stat.S_IFDIR | 0755
		else:
			st.st_mode = stat.S_IFREG | 0777
			st.st_nlink = 1

		myfs.set_id(path, self.gid, self.uid,st.st_mode,st.st_nlink)


	else:
		print "The file %s exists!!" %path
	return 0

    def write(self, path, data, offset):
	flag = 2
	write_status = self.getPermission(path,flag)
	if write_status == 1:
		print "In write path=%s" %path
		length = len(data)
		writeData = str(data)
		print "The data is %s len=%d offset=%d" %(writeData, length, offset)

		myfs = myFS()
		prevData = myfs.read(path)

		if prevData is not None:
			writeData = str(prevData) + writeData

		ret = myfs.write(path, writeData)
	
		current_time = int(time.time())
		ret = myfs.set_writetime(path, current_time)

		return length
	else:
		print "No write permission"
		return -errno.EACCES

    def read(self, path, size, offset):
	flag = 1
	read_status = self.getPermission(path,flag)
	if read_status == 1:
		try:
			print "In read %s" %path

			myfs = myFS()
			ret = myfs.read(path)
			print "read(): %s" %(ret)

			if ret is not None:
				fbuf = StringIO()
				fbuf.write(str(ret))
				output = fbuf.getvalue()
				fbuf.close()
				return output
			else:
				return ""

		except Exception, e:
			print "read failed"
			return e
	else:		
		print "No read permission"
		return -errno.EACCES

    def getPermission(self, path,flag):
		myfs = myFS()
		mode = myfs.getmode(path)
		
		#read-write and read-write-excute disable		
		if (mode == stat.S_IFREG | 0077) or (mode == stat.S_IFREG | 0177):
			return False
		
		if flag == 1:		
			#read and read-excute disable
			if (mode == stat.S_IFREG | 0277) or (mode == stat.S_IFREG | 0377):
				return False			
		else:		
			#write and write-execute disable
			if (mode == stat.S_IFREG | 0477) or (mode == stat.S_IFREG | 0577):
				return False			

		return True

    def access(self, path, flag):
        print "access path=%s" %path
	myfs = myFS()
	if myfs.search(path) is True:
		print "In access, found the file %s" %path
		return 0
	else:
		print "Could not find the file %s" %path
		return -errno.EACCES

    def chmod(self, path, mode):
	print "In chmod %s %s" %(path, str(mode))
	self.myfs.setmode(path, stat.S_IFREG | mode)
	current_time = int(time.time())
	self.myfs.set_writetime(path, current_time)
        return 0

    def utime(self, path, times=None):
	atime, mtime = times
	myfs = myFS()
	ret = myfs.utime(path, (atime, mtime, atime))
	return 0

    def unlink(self,path):
	print "In unlink path %s" %path
	myfs = myFS()
	ret = myfs.remove(path)
	return

    def fgetattr(path, fh=None):
	print "In fgetattr"
	return 0

    def release(self, path, flags):
	print "Close Function"
	if self.is_dirty is True:
		print "Flushing buffer"
		print self.buf.read()
		myfs = myFS()
		ret = myfs.write(path, self.buf.read())
		print self.buf.read()
		self.buf.close()
		del self.buf
		self.is_dirty = False
		print ret
	return 0

    def fsinit(self):
        print "Starting new file system..."

def main():
    if ((os.getuid() == 0) or (os.geteuid() == 0)):
	print "Error: Running file system as root opens unnacceptable security holes."
    	sys.exit(0)
    else:	
    	server = MyFS()
    	server.parse(errex=1)
    	server.main()


if __name__ == '__main__':
  	main()

########################################################################################################
