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

def scanWebPhotos(webAlbum):
	photos = repeat(lambda: gd_client.GetFeed(webAlbum.GetPhotosUri() + "&imgmax=d"),
		"list photos in album", True)
	for photo in photos.entry:
		photoDate = datetime.fromtimestamp(int(photo.timestamp.text[0:10])).strftime('%Y/%m/%d')
		print photo.size.text 
        	photoTitle = urllib.unquote(photoDate + "/" + photo.title.text)
		print photoTitle
		file = FileEntry(photo.title.text, photoTitle, None, False, photo)
		file.print_entry()

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
class FileEntry:
    def __init__(self, name, path, webReference, isLocal, album):
        self.name = name
        if path:
            self.path = path
            self.type = mimetypes.guess_type(path)[0]
        else:
            self.path = os.path.join(album.rootPath, name)
            self.type = None
        self.isLocal = isLocal
        self.localHash = None
        self.remoteHash = None
        self.remoteDate = None
        self.remoteTimestamp = None
        self.remoteSize = None
        self.album = album
        self.setWebReference(webReference)

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

            self.remoteHash = webReference.checksum.text
            self.remoteDate = calendar.timegm(
                time.strptime(re.sub("\.[0-9]{3}Z$", ".000 UTC", webReference.updated.text),
                              '%Y-%m-%dT%H:%M:%S.000 %Z'))
            self.remoteTimestamp = time.mktime(webReference.timestamp.datetime().timetuple())
            self.remoteSize = int(webReference.size.text)
        else:
            self.webUrl = None

    def print_entry(self):
	print self.webUrl
	print self.isLocal
       	print self.localHash
       	print self.remoteHash
       	print self.remoteDate
       	print self.remoteTimestamp 
       	print self.remoteSize 
       	print self.album 



# Main Program code
parser = argparse.ArgumentParser()
parser.add_argument("-d", "--directory", nargs='+',
                    help="The local directories. The first of these will be used for any downloaded items")
parser.add_argument("-t", "--test", default=False, action='store_true',
                   help="Don't actually run activities, but report what you would have done (you may want to enable verbose)")
parser.add_argument("-o", "--owner", default="default",
                    help="The username of the user whos albums to sync (leave blank for your own)")
parser.add_argument("-v", "--verbose", default=False, action='store_true', help="Increase verbosity")

args = parser.parse_args()

rootDirs = args.directory  # set the directory you want to sync to
verbose = args.verbose

gd_client = oauthLogin()

# walk the web album finding albums there
webAlbums = gd_client.GetUserFeed(user=args.owner)

for webAlbum in webAlbums.entry:
	webAlbumTitle = flatten(webAlbum.title.text)
	print ('Scanning web-album %s (containing %s files)' % (webAlbum.title.text, webAlbum.numphotos.text))
	scanWebPhotos(webAlbum)

