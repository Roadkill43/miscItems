#!/usr/bin/python

import os
import re
import argparse
import mimetypes
import hashlib
import time
import urllib
import fnmatch
import tempfile
import calendar
import httplib2
import threading
import requests

# from apiclient import discovery
from oauth2client import client
from subprocess import call
from datetime import timedelta, datetime

from gdata.photos.service import *
import gdata.media
import gdata.geo
from PIL import Image

def oauthLogin():
	# using http://stackoverflow.com/questions/20248555/list-of-spreadsheets-gdata-oauth2/29157967#29157967 (thanks)
	from oauth2client.file import Storage

	filename = os.path.join(os.path.expanduser('~'), ".picasawebsync")
	storage = Storage(filename)
	credentials = storage.get()
	if credentials is None or credentials.invalid:
		flow = client.flow_from_clientsecrets('client_secrets.json',scope='https://picasaweb.google.com/data/',redirect_uri='urn:ietf:wg:oauth:2.0:oob')	
		auth_uri = flow.step1_get_authorize_url()
		print 'Authorization URL: %s' % auth_uri
		auth_code = raw_input('Enter the auth code: ')
		credentials = flow.step2_exchange(auth_code)
		storage.put(credentials)
	# if credentials.access_token_expired:

	return refreshCreds(credentials,0)

def refreshCreds(credentials,sleep):
        global gd_client
        time.sleep(sleep)
	credentials.refresh(httplib2.Http())

	now = datetime.utcnow()
 	expires = credentials.token_expiry
	expires_seconds = (expires-now).seconds
	# print ("Expires %s from %s = %s" % (expires,now,expires_seconds) )

	gd_client = gdata.photos.service.PhotosService(email='default',additional_headers={'Authorization' : 'Bearer %s' % credentials.access_token})

	d = threading.Thread(name='refreshCreds', target=refreshCreds, args=(credentials,expires_seconds - 10) )
	d.setDaemon(True)
	d.start()
	return gd_client

def flatten(name):
        return re.sub("#[0-9]*$", "", name).rstrip()


# determine what webphotos exist are 
def scanWebPhotos(webAlbum, rootDirs, webAlbumTitle):
    photos = repeat(lambda: gd_client.GetFeed(webAlbum.GetPhotosUri() + "&imgmax=d"), "list photos in album", True)
    for photo in photos.entry:
        photoDate = datetime.fromtimestamp(int(photo.timestamp.text[0:10])).strftime('%Y/%m/%d')
        photoTitle = urllib.unquote(photoDate + "/" + photo.title.text)

        localPhotoPath = rootDirs + "/"
        file = RemoteFileEntry(photo.title.text, localPhotoPath, photo, photoDate, webAlbumTitle)

        if verbose or debug:
                print " %s -" % photoTitle,
                if debug:
                    print localPhotoPath
                    file.print_entry()

        # if the entry doesn't exist yet, add it. other wise, skip
        entryCaptured = False
        iteration = 0
        while not entryCaptured:
            iteration += 1
            if (not photoTitle in onlineEntries):
                onlineEntries[photoTitle] = file
                entryCaptured = True
                if verbose or debug:
                    print "added to list"
            else:
                if file.getAlbum() == onlineEntries[photoTitle].getAlbum() and file.getHash() != onlineEntries[photoTitle].getHash():
                    # add with an additiona (1) in the name
                    file.updateName(iteration)
                    photoTitle = urllib.unquote(photoDate + "/" + file.getName())
                    if verbose or debug:
                        print "Dup, renaming to %s" % photoTitle

                elif file.getAlbum() == "" and onlineEntries[photoTitle].getAlbum() != "" and file.getHash() == onlineEntries[photoTitle].getHash():
                    # skip entry
                    entryCaptured = True
                    if verbose or debug:
                        print "already exists in album"
                elif file.getAlbum() != "" and file.getAlbum() != onlineEntries[photoTitle].getAlbum() and file.getHash() == onlineEntries[photoTitle].getHash():
                    #file exists in multiple albums
                    entryCaptured = True
                    photoTitle = urllib.unquote(file.getAlbum() + photoDate + "/" + file.getName())
                    onlineEntries[photoTitle] = file
                    entryCaptured = True
                    if verbose or debug:
                        print "added to list, new album"
                else:
                    entryCaptured = True
                    print "error, shouldn't happen"

# repeat function
def repeat(function,  description, onFailRethrow):
	exc_info = None
	for attempt in range(3):
		try:
			if verbose and (attempt > 0):
				print ("Trying %s attempt %s" % (description, attempt) )
			return function()
		except Exception,  e:
			if exc_info == None:
				exc_info = e
			# FIXME - to try and stop 403 token expired
			time.sleep(6)
			# this should no longer be needed
			# gd_client=oauthLogin()
			continue
		else:
			break
	else:
		print ("WARNING: Failed to %s. This was due to %s" % (description, exc_info))
		if onFailRethrow:
			raise exc_info
#Class for local album scanning
class localFolder:
    def __init__(self, rootDirs):
        self.rootDirs = rootDirs
        self.entries = {}
        self.albums = self.scanFileSystem()

 # walk the directory tree populating the list of files we have locally
    # @print_timing
    def scanFileSystem(self):
        fileAlbums = {}
        for dirName, subdirList, fileList in os.walk(rootDirs):
            subdirList[:] = [d for d in subdirList]
            albumName = dirName
            # have we already seen this album? If so append our path to it's list
            if albumName in fileAlbums:
                album = fileAlbums[albumName]
                thisRoot = album.suggestNewRoot(dirName)
            else:
                # create a new album
                thisRoot = dirName
                album = AlbumEntry(dirName, albumName)
                fileAlbums[albumName] = album
            # now iterate it's files to add them to our list
            for fname in fileList:
                fullFilename = os.path.join(dirName, fname)
                relFileName = re.sub("^/", "", fullFilename[len(thisRoot):])
                fileEntry = LocalFileEntry(relFileName, fullFilename, albumName)
                   
                if ( albumsOnly and fileEntry.getLocalAlbumName() == ""  ) or fileEntry.getType() == None:
                    continue  # ignoring the local album or file types of None.
                if debug: 
                    print "--------"
                    fileEntry.print_entry()

                self.entries[fileEntry.getLocalPhotoPath()] = fileEntry
        if verbose:
            print ("Found " + str(len(fileAlbums)) + " albums on the filesystem")
        return fileAlbums;

    def getAlbums(self):
        return self.albums

    def getEntries(self):
        return self.entries

class AlbumEntry:
    def __init__(self, fileName, albumName):
        self.paths = [fileName]
        self.rootPath = fileName
        self.albumName = albumName
        self.entries = {}
        self.webAlbum = []
        self.webAlbumIndex = 0
        self.earliestDate = None

    def returnEnteries(self):
        return self.entries

# Class to store details of an individual file
class LocalFileEntry:
    def __init__(self, name, path, album):
        self.name = name
        if path:
            self.path = path
            self.type = mimetypes.guess_type(path)[0]
            if str("video") in str(self.type):
                self.type = "video"
        else:
            self.path = os.path.join(album.rootPath, name)
            self.type = None
        self.localHash = None
        self.localSize = os.path.getsize(self.path)
        self.localDate = os.path.getmtime(self.path)
        self.album = album

        self.localPhotoPath = self.path.replace(rootDirs+"/",'')
        self.localAlbumName = self.album.replace(rootDirs+"/",'')
        if self.localAlbumName == datetime.fromtimestamp(self.localDate).strftime('%Y/%m/%d'):
            self.localAlbumName = ""
        self.setHash()

    def setHash(self):
        if self.type == "video":
            #don't include the file size on video hash
            self.hash = hash(str(self.name) + str(self.localDate))
            self.albumHash = hash(str(self.localAlbumName) + str(self.name) + str(self.localDate))
        else:            
            self.hash = hash(str(self.name) + str(self.localSize) + str(self.localDate))
            self.albumHash = hash(str(self.localAlbumName) + str(self.name) + str(self.localSize) + str(self.localDate))

    def print_entry(self):
        print ("name %s" % self.name)
        print ("album %s" % self.album)
        print ("localAlbumName %s" % self.localAlbumName)
        print ("path %s" % self.path)
        print ("localPhotoPath %s " % self.localPhotoPath)
        print ("localSize %s" % self.localSize)
        print ("Localdate %s" % self.localDate)
        print ("Album Hash %s" % self.albumHash)
        print ("hash %s" % self.hash)
        print ("type %s" % self.type)

    def getName(self):
        return self.name

    def getType(self):
        return self.type

    def getLocalAlbumName(self):
        return self.localAlbumName

    def getAlbumHash(self):
        return self.albumHash

    def getLocalPhotoPath(self):
        return self.localPhotoPath

    def getFullPath(self):
        return self.path
# ----------------------------------------------#
# Class to compare online vs local
# ----------------------------------------------#
class compareWebandLocal:
    def __init__(self,localFolder):
        self.toDownload = {}
        self.localDelete = {}
        self.purgedCount = 0
        self.downloadCount = 0
        self.onlineEntries = onlineEntries
        self.localEntries = localFolder.getEntries()
        self.compare()

    def compare(self):
        # loop through online items to create list that is missing
        for photoTitle in self.onlineEntries:
            localFound = False
            file = self.onlineEntries[photoTitle]

            fileAlbumHash = file.getAlbumHash()

            for localEntry in self.localEntries:

                if fileAlbumHash == self.localEntries[localEntry].getAlbumHash():
                    #entry found,
                    localFound = True
                    if debug:
                        print "Matched local vs web %s" % self.localEntries[localEntry].getName()
                    del self.localEntries[localEntry]
                    break 
            
            if localFound == False:
                if verbose or debug:
                    print "added to download: Local not found %s" % file.getName(),
                    print "path %s" % file.getAlbum()
                self.toDownload[photoTitle] = file

        if debug or verbose:
            print '--- Local Entries to be purged ---'
            for localEntry in self.localEntries:
                self.localEntries[localEntry].print_entry()
            print '--- web Entries to be downloaded ---'
            for toDownload in self.toDownload:
                self.toDownload[toDownload].print_entry()

    def printStatsPre(self):
        self.preTime = datetime.now()
        print '###########################################'
        print "# Items missing on local storage  : %s" % str(len(self.toDownload))
        print "# Mis-match items, i.e. not online: %s" % str(len(self.localEntries))
    def printStatsPost(self):
        self.postTime = datetime.now()
        print '#-----------------------------------------#'
        print '# Items downloaded: %s' % self.downloadCount
        print '# Items purged    : %s' % self.purgedCount
        print '#-----------------------------------------#'
        print '# Start Time      : %s' % str(self.preTime)
        print '# End Time        : %s' % str(self.postTime)
        print '# total runtime   : %s' % str(self.postTime - self.preTime)
        print '###########################################'
    def download(self):
        # Download the photos from online
        for photoTitle in self.toDownload:
            file =  self.toDownload[photoTitle]
            if verbose:
                print 'Downloading %s' % file.getName()
            if file.download_file():
                self.downloadCount += 1

    def purge(self):
        #purge files.
        for photoTitle in self.localEntries:
            file = self.localEntries[photoTitle]
            if verbose:
                print 'purging %s' % file.getFullPath()
            if not test:
                try:
                    os.remove(file.getFullPath())
                    self.purgedCount += 1
                except OSError as err:
                    if debug:
                        print("OS error: {0}".format(err))
                    continue
            else:
                print "Test mode -Local file %s to be purged" % file.getFullPath()
        #remove blank folders
        self.removeEmptyFolders(rootDirs + "/")


    def removeEmptyFolders(self, path, removeRoot = True):
        'Function to remove empty folders'
        if not os.path.isdir(path):
            return
        # remove empty subfolders
        files = os.listdir(path)
        if len(files):
            for f in files:
                fullpath = os.path.join(path, f)
                if os.path.isdir(fullpath):
                    self.removeEmptyFolders(fullpath)

        # if folder empty, delete it
        files = os.listdir(path)
        if len(files) == 0 and removeRoot:
            if not test:
                print "Removing empty folder:", path
                os.rmdir(path)
            else:
                print "Test mode -Local folder %s to be removed" % path


# ----------------------------------------------#
# Class to store details of an individual file
# ----------------------------------------------#
class RemoteFileEntry:
    def __init__(self, name, path, webReference, datePath, album):
        self.name = name
        if path:
            self.path = path
            self.type = mimetypes.guess_type(path)[0]
            if str(self.type).startswith('video'):
                self.type = "video"
        else:
            self.path = os.path.join(album.rootPath, name)
            self.type = None
        self.remoteDate = None
        self.remoteTimestamp = None
        self.remoteSize = None
        self.album = album
        self.datePath = datePath
        self.setWebReference(webReference)

    def getAlbum(self):
        return self.album

    def getName(self):
        return self.name

    def getHash(self):
        return self.hash

    def getAlbumHash(self):
        return self.albumHash

    def updateName(self,Number):
        newName = self.name.rsplit('.',1)[0] +" ("+str(Number)+")."+self.name.rsplit('.',1)[1]
        self.name = newName
        self.setHash()

    def setHash(self):
        if self.type == "video":
            #don't include the file size on video hash'
            self.albumHash = hash(str(self.album) + str(self.name) +  str(self.remoteTimestamp))
            self.hash = hash(str(self.name) + str(self.remoteTimestamp))
        else:
            self.albumHash = hash(str(self.album) + str(self.name) + str(self.remoteSize) + str(self.remoteTimestamp))
            self.hash = hash(str(self.name) + str(self.remoteSize) + str(self.remoteTimestamp))  

    def setWebReference(self, webReference):
        if webReference:
            for content in webReference.media.content:
                # If we haven't found a type yet, or prioritise video type
                if not self.type or (content.medium == 'video'):
                    self.type = content.type
                if str(self.type).startswith('video'):
                    self.type = "video"

            self.gphoto_id = webReference.gphoto_id.text
            self.albumid = webReference.albumid.text
            self.webUrl = webReference.content.src

            #if video overwrite webUrl : get last (higher resolution) media.content entry url
            if webReference.media.content:
                highRescontent = webReference.media.content[-1]
                if highRescontent.type.startswith('video'):
                    if highRescontent.url:
                        self.webUrl = highRescontent.url

            self.remoteDate = calendar.timegm(
                time.strptime(re.sub("\.[0-9]{3}Z$", ".000 UTC", webReference.updated.text),
                              '%Y-%m-%dT%H:%M:%S.000 %Z'))
            self.remoteTimestamp = time.mktime(webReference.timestamp.datetime().timetuple())
            self.remoteSize = int(webReference.size.text)
            self.setHash()
        else:
            self.webUrl = None

    def print_entry(self):
        print ("name %s" % self.name)
        print ("datePath %s" % self.datePath)
        print ("album %s" % self.album)
        print ("albumid %s" % self.albumid)
        print ("path %s" % self.path)
        print ("webUrl %s" % self.webUrl)
        print ("Remote date %s" % self.remoteDate)
        print ("RemoteTimesamp %s" % self.remoteTimestamp) 
        print ("Remote size %s" % self.remoteSize)
        print ("Album Hash %s" % self.albumHash)
        print ("hash %s" % self.hash)
        print ("type %s" % self.type)

    def download_file(self):
        url = self.webUrl
        if self.album == "":
            local_filename = self.path + self.datePath+ "/" + self.name
            localPath = self.path + self.datePath
        else:
            local_filename = self.path + self.album + "/" + self.name
            localPath = self.path + self.album
            
        #check to see if the file exists already locally
        if os.path.isfile(local_filename):
            if verbose or debug:
                print "Checking...%s already exists locally" % local_filename
            self.localSize = os.path.getsize(local_filename)
            self.localDate = os.path.getmtime(local_filename)
            self.localDate = self.localDate
        else:
            self.localSize = 0
            self.localDate = 0
        #check for date or size difference
        if self.localDate != self.remoteTimestamp or (self.type =='photo' and self.localSize != self.remoteSize):
            if verbose or debug:
                print "downloading %s" % local_filename
                if debug:
                    print "---Local Date %s, Remote Date %s ---" % (self.localDate, self.remoteTimestamp)
                    print "---Local size %s, Remote size %s ---" % (self.localSize, self.remoteSize)
                    print "-- Type %s" % self.type
            # NOTE the stream=True parameter
            if not test:
                r = requests.get(url, stream=True)
                if not os.path.exists(localPath):
                        os.makedirs(localPath)
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=24 * 1024): 
                        if chunk: # filter out keep-alive new chunks
                            f.write(chunk)
                            #f.flush() commented by recommendation from J.F.Sebastian
                os.utime(local_filename, (int(self.remoteTimestamp), int(self.remoteTimestamp)))
                self.localSize = os.path.getsize(local_filename)
                self.localDate = os.path.getmtime(local_filename)
                self.localDate = self.localDate
                #confirm the download was succesful
                if self.localDate == self.remoteTimestamp and (self.localSize == self.remoteSize or self.type =='video'):
                    if verbose:
                        print "Success: download of %s completed" % local_filename
                    return True
                else:
                    if verbose or debug:
                        print "Error when downloading %s" % local_filename
                        if debug: 
                                print "---Local Date %s, Remote Date %s ---" % (self.localDate, self.remoteTimestamp)
                                print "---Local size %s, Remote size %s ---" % (self.localSize, self.remoteSize)
                    return False
            else:
                print "Test mode -file %s to be downloaded" % local_filename
        else:
            if verbose or debug:
                print "skipping File %s already downloaded" % local_filename

# Get the web albums that are available. 
def getWebFiles(webAlbums):
    #First check for webalbums, then do immutableFolders
    for webAlbum in webAlbums.entry:
        webAlbumTitle = flatten(webAlbum.title.text)
        if not webAlbum.title.text in immutableFolders:
            if verbose or debug:
                print ('Scanning web-album %s (containing %s files)' % (webAlbum.title.text, webAlbum.numphotos.text))
            scanWebPhotos(webAlbum, rootDirs, webAlbumTitle)

    if not albumsOnly:  #skip the immutableFolders if arg albums only is set
        for webAlbum in webAlbums.entry:
            webAlbumTitle = flatten(webAlbum.title.text)
            if webAlbum.title.text in immutableFolders:
                if verbose or debug:
                    print ('Scanning web-album %s (containing %s files)' % (webAlbum.title.text, webAlbum.numphotos.text))
                scanWebPhotos(webAlbum, rootDirs, "") # don't pass an album name

# -------------------------------#
# Main Program code
# -------------------------------#
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory",
                    help="The local directories. The first of these will be used for any downloaded items")
parser.add_argument("-t", "--test", default=False, action='store_true',
                   help="Don't actually run activities, but report what you would have done (you may want to enable verbose)")
parser.add_argument("-o", "--owner", default="default",
                    help="The username of the user whos albums to sync (leave blank for your own)")
parser.add_argument("-v", "--verbose", default=False, action='store_true', help="Increase verbosity")
parser.add_argument("-D", "--debug", default=False, action='store_true', help="Debug mode")
parser.add_argument("-a", "--albums", default=False, action='store_true', help="Albums only, ignore auto backup, instant upload folders")
parser.add_argument("-s", "--stats", default=False, action='store_true', help="Print the stats of what happened")
parser.add_argument("--nopurge", default=False, action='store_true', help="Do not purge local files and folders that don't match with what is on the web")
parser.add_argument("--nosync", default=False, action='store_true', help="Do not download items that are missing locally (default is to download)")



args = parser.parse_args()
# Global data Declerations #
immutableFolders = frozenset(["Instant Upload","Auto-Backup","Auto Backup"])
onlineEntries = {}   #online photos/videos


# Arguments
rootDirs = args.directory  # set the directory you want to sync to
stats = args.stats   #print the stats for what will synced.
verbose = args.verbose
debug = args.debug
test = args.test
albumsOnly = args.albums
nopurge = args.nopurge
nosync = args.nosync

#Main processing
gd_client = oauthLogin()

# walk the web album finding albums there
webAlbums = gd_client.GetUserFeed(user=args.owner)

#get web files
getWebFiles(webAlbums)

#get local files
localFiles = localFolder(rootDirs)

#compare online vs local
comparedObjects = compareWebandLocal(localFiles)
if stats:
    comparedObjects.printStatsPre()
#Purge objects if the purge flag is set
#   this is done first to clean any files that might be partially downloaded
if not nopurge:
    comparedObjects.purge()
#Download if nosync is not set
if not nosync:
    comparedObjects.download()
#print stats
if stats:
    comparedObjects.printStatsPost()
