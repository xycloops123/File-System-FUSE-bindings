#############################################################################
# Database interface for file system.
# AUTHOR	: Gaurav Mukherjee, NYU POLY	
# CREATED DATED	: 12/08/2013
# FILE		: myFS.py
# PURPOSE	: Store metadata of file(s) in database.
# Version	: 1.0                                             
#############################################################################

from db.SQLiteHandler import SQLiteHandler
import time,bz2, random,sys

class myFS:
	def __init__(self):
		# Connects to myFS database

		self.sql = SQLiteHandler('fsDB')
		self.sql.connect()
		
	def ls(self):
		# Implements ls operation

		rows = self.sql.execute(''' select * from metadata ''')
		list = []
		for row in rows:
			list.append(row[1])
		return list

	def open(self, path):
		""" insert new record in database """

		self.sql.execute(''' insert into metadata (abspath) select '%s' where not exists (SELECT 1 FROM metadata WHERE abspath='%s') ''' %(path,path))
		self.sql.commit()
		return path
	
	def write(self, path, data):
		""" Implements write() call """
		length  = len(data)

		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(path) )
		id = None
		for row in rows:
			id = row[0]

			self.sql.execute(''' update metadata set length= '%s',data= '%s' where abspath='%s' ''' %(length, data, path))
			self.sql.commit()
			return

	def getutime(self, path):
		# Getter for utime
		rows = self.sql.execute('''select mtime, ctime, atime from metadata where abspath='%s' ''' %( path))
		for row in rows:
			tup = (row[0], row[1], row[2])
			return tup

	def utime(self, path, times):
		# Setter for utime
		mtime, ctime, atime = times
		self.sql.execute(''' update metadata  set mtime='%s', ctime='%s', atime='%s' where abspath='%s' ''' %(mtime, ctime, atime, path))
		self.sql.commit()
		return times

	def setinode(self,path,inode):
		# Setter for inode
		self.sql.execute(''' update metadata  set inode = '%d' where abspath='%s' ''' %(inode, path))
		self.sql.commit()
		return inode

	def set_id(self,path,gid,uid,mode,link):
		# Setter for uid, gid, mode and linkcount
		self.sql.execute(''' update metadata  set gid= '%d',uid= '%d',mode='%d',linkcount='%d' where abspath='%s' ''' %(gid, uid, mode,link, path))
		self.sql.commit()
		return True

	def setlinkcount(self,path,linkcnt):
		# Setter for linkcount
		self.sql.execute(''' update metadata  set linkcount = '%d' where abspath='%s' ''' %(linkcnt, path))
		self.sql.commit()
		return linkcnt

	def setmode(self,path,mode):
		# Setter for mode
		self.sql.execute(''' update metadata  set mode = '%d' where abspath='%s' ''' %(mode, path))
		self.sql.commit()
		return mode

	def getinode(self,path):
		# Getter for inode
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]

	def getlength(self,path):
		# Calculates length of data
		rows = self.sql.execute(''' select length from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]

	def getlinkcount(self,path):
		# Getter for linkcount
		rows = self.sql.execute(''' select linkcount from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]
	def getmode(self,path):
		# Getter for mode
		rows = self.sql.execute(''' select mode from metadata where abspath='%s' ''' %( path))
		for row in rows:
			return row[0]


	def set_writetime(self, path, times):

		self.sql.execute(''' update metadata  set mtime='%s', atime='%s' where abspath='%s' ''' %(times, times, path))
		self.sql.commit()
		return times
		
	def remove(self,abspath):
		# Implements unlink() call
		self.sql.execute(''' delete from metadata where abspath='%s' ''' %(abspath))
		self.sql.commit()
		return
	
	def search(self, abspath):
		# Search file in database record
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(abspath))
		if rows.fetchall():
			return True
		return False

	def read(self, fh):
		# Implements read() call
		id  = None
		rows = self.sql.execute(''' select inode from metadata where abspath='%s' ''' %(fh))
		for row in rows:
			id = row[0]

		if id is None:
			return None
		rows = self.sql.execute(''' select data from metadata where abspath='%s' ''' %(fh))
		for row in rows:
			return row[0]
		
		
	def __del__(self):
		# Closes the databse connection
		self.sql.close()
########################################################################################################
