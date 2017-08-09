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

        localPhotoPath = rootDirs[0] + "/"
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


                
# Download the photos from online
def downloadWebPhotos():
    # download files
    for photoTitle in onlineEntries:
        file = onlineEntries[photoTitle]
        file.download_file()

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

# Class to store details of an individual file
class RemoteFileEntry:
    def __init__(self, name, path, webReference, datePath, album):
        self.name = name
        if path:
            self.path = path
            self.type = mimetypes.guess_type(path)[0]
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
        self.albumHash = hash(str(self.album) + str(self.name) + str(self.remoteSize) + str(self.remoteTimestamp))

    def setWebReference(self, webReference):
        if webReference:
            for content in webReference.media.content:
                # If we haven't found a type yet, or prioritise video type
                if not self.type or (content.medium == 'video'):
                    self.type = content.type

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
            self.hash = hash(str(self.name) + str(self.remoteSize) + str(self.remoteTimestamp))
            self.albumHash = hash(str(self.album) + str(self.name) + str(self.remoteSize) + str(self.remoteTimestamp))
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
                else:
                    if verbose or debug:
                        print "Error when downloading %s" % local_filename
                        if debug: 
                                print "---Local Date %s, Remote Date %s ---" % (self.localDate, self.remoteTimestamp)
                                print "---Local size %s, Remote size %s ---" % (self.localSize, self.remoteSize)
            else:
                print "Test mode -file %s to be downloaded" % local_filename
        else:
            if verbose or debug:
                print "skipping File %s already downloaded" % local_filename


# -------------------------------#
# Main Program code
# -------------------------------#
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", nargs='+',
                    help="The local directories. The first of these will be used for any downloaded items")
parser.add_argument("-t", "--test", default=False, action='store_true',
                   help="Don't actually run activities, but report what you would have done (you may want to enable verbose)")
parser.add_argument("-o", "--owner", default="default",
                    help="The username of the user whos albums to sync (leave blank for your own)")
parser.add_argument("-v", "--verbose", default=False, action='store_true', help="Increase verbosity")
parser.add_argument("-D", "--debug", default=False, action='store_true', help="Debug mode")
parser.add_argument("-a", "--albums", default=False, action='store_true', help="Albuums only, ignore auto backup, instant upload folders")

args = parser.parse_args()
# Global data Declerations #
immutableFolders = frozenset(["Instant Upload","Auto-Backup","Auto Backup"])
onlineEntries = {}   #online photos/videos

# Arguments
rootDirs = args.directory  # set the directory you want to sync to
verbose = args.verbose
debug = args.debug
test = args.test
albumsOnly = args.albums

gd_client = oauthLogin()

# walk the web album finding albums there
webAlbums = gd_client.GetUserFeed(user=args.owner)

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


downloadWebPhotos()
