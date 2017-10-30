# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import datetime
import hashlib
import inspect
import json
import logging
from lxml import etree
import os
import requests
import shutil
import subprocess
from sqlalchemy import create_engine
import re
import textwrap
import time
import uuid
import xmltodict

# django imports
from django.apps import AppConfig
from django.conf import settings
from django.contrib.auth.models import User
from django.contrib.auth import signals
from django.db import models
from django.dispatch import receiver
from django.utils.encoding import python_2_unicode_compatible
from django.utils.html import format_html

# Livy
from livy.client import HttpClient

# import elasticsearch and handles
from core.es import es_handle
from elasticsearch_dsl import Search, A, Q

# Get an instance of a logger
logger = logging.getLogger(__name__)



##################################
# Django ORM
##################################

class LivySession(models.Model):

	name = models.CharField(max_length=128)
	session_id = models.IntegerField()
	session_url = models.CharField(max_length=128)
	status = models.CharField(max_length=30, null=True)
	session_timestamp = models.CharField(max_length=128)
	appId = models.CharField(max_length=128, null=True)
	driverLogUrl = models.CharField(max_length=255, null=True)
	sparkUiUrl = models.CharField(max_length=255, null=True)
	active = models.BooleanField(default=0)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Livy session: %s, status: %s' % (self.name, self.status)


	def refresh_from_livy(self):

		'''
		ping Livy for session status and update DB
		'''

		logger.debug('querying Livy for session status')

		# query Livy for session status
		livy_response = LivyClient().session_status(self.session_id)

		# parse response and set self values
		logger.debug(livy_response.status_code)
		response = livy_response.json()
		logger.debug(response)
		headers = livy_response.headers
		logger.debug(headers)

		# if status_code 404, set as gone
		if livy_response.status_code == 404:
			
			logger.debug('session not found, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		elif livy_response.status_code == 200:
			
			# update Livy information
			logger.debug('session found, updating status')
			
			# update status
			self.status = response['state']
			if self.status in ['starting','idle','busy']:
				self.active = True
			
			self.session_timestamp = headers['Date']
			
			# update Spark/YARN information, if available
			if 'appId' in response.keys():
				self.appId = response['appId']
			if 'appInfo' in response.keys():
				if 'driverLogUrl' in response['appInfo']:
					self.driverLogUrl = response['appInfo']['driverLogUrl']
				if 'sparkUiUrl' in response['appInfo']:
					self.sparkUiUrl = response['appInfo']['sparkUiUrl']
			# update
			self.save()

		else:
			
			logger.debug('error retrieving information about Livy session')


	def stop_session(self):

		'''
		Stop Livy session with Livy HttpClient
		'''

		# stop session
		LivyClient.stop_session(self.session_id)

		# update from Livy
		self.refresh_from_livy()


	@staticmethod
	def get_active_session():

		'''
		Convenience method to return single active livy session,
		or multiple if multiple exist
		'''

		active_livy_sessions = LivySession.objects.filter(active=True)

		if active_livy_sessions.count() == 1:
			return active_livy_sessions.first()

		elif active_livy_sessions.count() == 0:
			logger.debug('no active livy sessions found, returning False')
			return False

		elif active_livy_sessions.count() > 1:
			logger.debug('multiple active livy sessions found, returning as list')
			return active_livy_sessions



class Organization(models.Model):

	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255)
	publish_id = models.CharField(max_length=255)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return 'Organization: %s' % self.name



class RecordGroup(models.Model):

	organization = models.ForeignKey(Organization, on_delete=models.CASCADE)
	name = models.CharField(max_length=128)
	description = models.CharField(max_length=255, null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)
	publish_set_id = models.CharField(max_length=128)


	def __str__(self):
		return 'Record Group: %s' % self.name


	def get_published_sets(self):

		'''
		Query DB for jobs published as sets for all record groups
		'''



class Job(models.Model):

	record_group = models.ForeignKey(RecordGroup, on_delete=models.CASCADE)
	job_type = models.CharField(max_length=128, null=True)
	user = models.ForeignKey(User, on_delete=models.CASCADE)
	name = models.CharField(max_length=128, null=True)
	spark_code = models.TextField(null=True, default=None)
	job_id = models.IntegerField(null=True, default=None)
	status = models.CharField(max_length=30, null=True)
	finished = models.BooleanField(default=0)
	url = models.CharField(max_length=255, null=True)
	headers = models.CharField(max_length=255, null=True)
	response = models.TextField(null=True, default=None)
	job_output = models.TextField(null=True, default=None)
	# job_output_filename_hash = models.CharField(max_length=255, null=True)
	record_count = models.IntegerField(null=True, default=0)
	published = models.BooleanField(default=0)
	job_details = models.TextField(null=True, default=None)
	timestamp = models.DateTimeField(null=True, auto_now_add=True)


	def __str__(self):
		return '%s, Job #%s, from Record Group: %s' % (self.name, self.id, self.record_group.name)


	def refresh_from_livy(self):

		# query Livy for statement status
		livy_response = LivyClient().job_status(self.url)
		
		# if status_code 404, set as gone
		if livy_response.status_code == 400:
			
			logger.debug(livy_response.json())
			logger.debug('Livy session likely not active, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		# if status_code 404, set as gone
		if livy_response.status_code == 404:
			
			logger.debug('job/statement not found, setting status to gone')
			self.status = 'gone'
			# update
			self.save()

		elif livy_response.status_code == 200:

			# parse response
			response = livy_response.json()
			headers = livy_response.headers
			
			# update Livy information
			logger.debug('job/statement found, updating status')
			self.status = response['state']

			# if state is available, assume finished
			if self.status == 'available':
				self.finished = True

			# update
			self.save()

		else:
			
			logger.debug('error retrieving information about Livy job/statement')
			logger.debug(livy_response.status_code)
			logger.debug(livy_response.json())


	def get_records(self):

		'''
		retrieve records for this job from DB
		'''

		return Record.objects.filter(job=self).exclude(document='').all()


	def get_errors(self):

		'''
		retrieve errors for this job from DB
		'''

		return Record.objects.filter(job=self).exclude(error='').all()


	def update_record_count(self):

		'''
		Get record count from DB, save to Job
		'''
		
		self.record_count = self.get_records().count()
		self.save()


	def job_output_as_filesystem(self):

		'''
		Not entirely removing the possibility of storing jobs on HDFS, 
		this method returns self.job_output as filesystem location
		and strips any righthand slash
		'''

		return self.job_output.replace('file://','').rstrip('/')


	def get_output_files(self):

		'''
		convenience method to return full path of all avro files in job output
		'''

		output_dir = self.job_output_as_filesystem()
		return [ os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith('.avro') ]


	def index_results_save_path(self):

		'''
		return index save path
		'''
		
		# index results save path
		return '%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (settings.BINARY_STORAGE.rstrip('/'), self.record_group.organization.id, self.record_group.id, self.id)



class JobInput(models.Model):

	'''
	Provides a one-to-many relationship for a job and potential multiple input jobs
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	input_job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name='input_job')



class JobPublish(models.Model):

	'''
	Provides a one-to-one relationship for a record group and published job
	'''

	record_group = models.ForeignKey(RecordGroup)
	job = models.ForeignKey(Job, on_delete=models.CASCADE)

	def __str__(self):
		return 'Published Set #%s, "%s" - from Job %s, Record Group %s - ' % (self.id, self.record_group.publish_set_id, self.job.name, self.record_group.name)



class OAIEndpoint(models.Model):

	name = models.CharField(max_length=255)
	endpoint = models.CharField(max_length=255)
	verb = models.CharField(max_length=128)
	metadataPrefix = models.CharField(max_length=128)
	scope_type = models.CharField(max_length=128) # expecting one of setList, whiteList, blackList
	scope_value = models.CharField(max_length=1024)


	def __str__(self):
		return 'OAI endpoint: %s' % self.name



class Transformation(models.Model):

	name = models.CharField(max_length=255)
	payload = models.TextField()
	transformation_type = models.CharField(max_length=255, choices=[('xslt','XSLT Stylesheet'),('python','Python Code Snippet')])
	filepath = models.CharField(max_length=1024, null=True, default=None)
	

	def __str__(self):
		return 'Transformation: %s, transformation type: %s' % (self.name, self.transformation_type)



class OAITransaction(models.Model):

	verb = models.CharField(max_length=255)
	start = models.IntegerField(null=True, default=None)
	chunk_size = models.IntegerField(null=True, default=None)
	publish_set_id = models.CharField(max_length=255, null=True, default=None)
	token = models.CharField(max_length=1024, db_index=True)
	args = models.CharField(max_length=1024)
	

	def __str__(self):
		return 'OAI Transaction: %s, resumption token: %s' % (self.id, self.token)



class Record(models.Model):

	'''
	DB model for individual records.
	Note: This DB model is not managed by Django.
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	index = models.IntegerField(null=True, default=None)
	record_id = models.CharField(max_length=1024, null=True, default=None)
	document = models.TextField(null=True, default=None)
	error = models.TextField(null=True, default=None)

	# this model is managed outside of Django
	class Meta:
		managed = False


	def __str__(self):
		return 'Record: #%s, record_id: %s, job_id: %s, job_type: %s' % (self.id, self.record_id, self.job.id, self.job.job_type)


	def get_record_stages(self, input_record_only=False):

		'''
		method to return all upstream and downstreams stages of this record
		'''

		record_stages = []

		def get_upstream(record, input_record_only):

			# check for upstream job
			uj_query = record.job.jobinput_set

			# if upstream jobs found, continue
			if uj_query.count() > 0:

				logger.debug('upstream jobs found, checking for record_id')

				# loop through upstream jobs, look for record id
				for uj in uj_query.all():
					ur_query = Record.objects.filter(job=uj.input_job).filter(record_id=self.record_id)

					# if count found, save record to record_stages and re-run
					if ur_query.count() > 0:
						ur = ur_query.first()
						record_stages.insert(0, ur)
						if not input_record_only:
							get_upstream(ur, input_record_only)


		def get_downstream(record):

			# check for downstream job
			dj_query = JobInput.objects.filter(input_job=record.job)

			# if downstream jobs found, continue
			if dj_query.count() > 0:

				logger.debug('downstream jobs found, checking for record_id')

				# loop through downstream jobs
				for dj in dj_query.all():

					dr_query = Record.objects.filter(job=dj.job).filter(record_id=self.record_id)

					# if count found, save record to record_stages and re-run
					if dr_query.count() > 0:
						dr = dr_query.first()
						record_stages.append(dr)
						get_downstream(dr)

		# run
		get_upstream(self, input_record_only)
		if not input_record_only:
			record_stages.append(self)
			get_downstream(self)
		
		# return		
		return record_stages



class IndexMappingFailure(models.Model):

	'''
	DB model for indexing failures
	Note: This DB model is not managed by Django.
	'''

	job = models.ForeignKey(Job, on_delete=models.CASCADE)
	record_id = models.CharField(max_length=1024, null=True, default=None)
	mapping_error = models.TextField(null=True, default=None)


	# this model is managed outside of Django
	class Meta:
		managed = False


	def __str__(self):
		return 'Index Mapping Failure: #%s, record_id: %s, job_id: %s' % (self.id, self.record_id, self.job.id)


	@property
	def record(self):

		'''
		method to return record the indexing failure stemmed from
		'''

		return Record.objects.filter(job=self.job, record_id=self.record_id).first()



##################################
# Signals Handlers
##################################

@receiver(signals.user_logged_in)
def user_login_handle_livy_sessions(sender, user, **kwargs):

	'''
	When user logs in, handle check for pre-existing sessions or creating
	'''

	# if superuser, skip
	if user.is_superuser:
		logger.debug("superuser detected, not initiating Livy session")
		return False

	# else, continune with user sessions
	else:
		logger.debug('Checking for pre-existing user sessions')

		# get "active" user sessions
		livy_sessions = LivySession.objects.filter(status__in=['starting','running','idle'])
		logger.debug(livy_sessions)

		# none found
		if livy_sessions.count() == 0:
			logger.debug('no Livy sessions found, creating')
			livy_session = LivySession().save()

		# if sessions present
		elif livy_sessions.count() == 1:
			logger.debug('single, active Livy session found, using')

		elif livy_sessions.count() > 1:
			logger.debug('multiple Livy sessions found, sending to sessions page to select one')



@receiver(models.signals.pre_save, sender=LivySession)
def create_livy_session(sender, instance, **kwargs):

	'''
	Before saving a LivySession instance, check if brand new, or updating status
		- if not self.id, assume new and create new session with POST
		- if self.id, assume checking status, only issue GET and update fields
	'''

	# not instance.id, assume new
	if not instance.id:

		logger.debug('creating new Livy session')

		# create livy session, get response
		livy_response = LivyClient().create_session()

		# parse response and set instance values
		response = livy_response.json()
		headers = livy_response.headers

		instance.name = 'Livy Session, sessionId %s' % (response['id'])
		instance.session_id = int(response['id'])
		instance.session_url = headers['Location']
		instance.status = response['state']
		instance.session_timestamp = headers['Date']
		instance.active = True



@receiver(models.signals.post_save, sender=Job)
def save_job(sender, instance, created, **kwargs):

	# if the record was just created, then update job output (ensures this only runs once)
	if created:
		# set output based on job type
		logger.debug('setting job output for job')
		instance.job_output = '%s/organizations/%s/record_group/%s/jobs/%s/%s' % (settings.BINARY_STORAGE.rstrip('/'), instance.record_group.organization.id, instance.record_group.id, instance.job_type, instance.id)
		instance.save()



@receiver(models.signals.pre_delete, sender=Job)
def delete_job_output_pre_delete(sender, instance, **kwargs):

	'''
	When jobs are removed, a fair amount of clean up is involved
	'''

	logger.debug('removing job_output for job id %s' % instance.id)

	# check if job running or queued, attempt to stop
	try:
		instance.refresh_from_livy()
		if instance.status in ['waiting','running']:
			# attempt to stop job
			livy_response = LivyClient().stop_job(instance.url)
			logger.debug(livy_response)

	except Exception as e:
		logger.debug('could not stop job in livy')
		logger.debug(str(e))


	# if publish job, remove symlinks to global /published
	if instance.job_type == 'PublishJob':

		logger.debug('Publish job detected, removing symlinks and removing record set from ES index')

		# open cjob
		cjob = CombineJob.get_combine_job(instance.id)

		# loop through published symlinks and look for filename hash similarity
		published_dir = os.path.join(settings.BINARY_STORAGE.split('file://')[-1].rstrip('/'), 'published')
		job_output_filename_hash = cjob.get_job_output_filename_hash()
		try:
			for f in os.listdir(published_dir):
				# if hash is part of filename, remove
				if job_output_filename_hash in f:
					os.remove(os.path.join(published_dir, f))
		except:
			logger.debug('could not delete symlinks from /published directory')

		# attempting to delete from ES
		try:
			del_dsl = {
				'query':{
					'match':{
						'publish_set_id':instance.record_group.publish_set_id
					}
				}
			}
			if es_handle.indices.exists('published'):
				r = es_handle.delete_by_query(
					index='published',
					doc_type='record',
					body=del_dsl
				)
			else:
				logger.debug('published index not found in ES, skipping removal of records')
		except Exception as e:
			logger.debug('could not remove published records from ES index')
			logger.debug(str(e))


	# remove avro files from disk
	# if file://
	if instance.job_output and instance.job_output.startswith('file://'):

		try:
			output_dir = instance.job_output.split('file://')[-1]
			shutil.rmtree(output_dir)
		except:
			logger.debug('could not remove job output directory at: %s' % instance.job_output)


	# remove ES index if exists
	try:
		if es_handle.indices.exists('j%s' % instance.id):
			logger.debug('removing ES index: j%s' % instance.id)
			es_handle.indices.delete('j%s' % instance.id)
	except:
		logger.debug('could not remove ES index: j%s' % instance.id)


	# attempt to delete indexing results avro files
	try:
		indexing_dir = ('%s/organizations/%s/record_group/%s/jobs/indexing/%s' % (settings.BINARY_STORAGE.rstrip('/'), instance.record_group.organization.id, instance.record_group.id, instance.id)).split('file://')[-1]
		shutil.rmtree(indexing_dir)
	except:
		logger.debug('could not remove indexing results')



@receiver(models.signals.pre_save, sender=Transformation)
def save_transformation_to_disk(sender, instance, **kwargs):

	'''
	When users enter a payload for a transformation, write to disk for use in Spark context
	'''

	# check that transformation directory exists
	transformations_dir = '%s/transformations' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]
	if not os.path.exists(transformations_dir):
		os.mkdir(transformations_dir)

	# if previously written to disk, remove
	if instance.filepath:
		try:
			os.remove(instance.filepath)
		except:
			logger.debug('could not remove transformation file: %s' % instance.filepath)

	# write XSLT type transformation to disk
	if instance.transformation_type == 'xslt':
		filename = uuid.uuid4().hex

		filepath = '%s/%s.xsl' % (transformations_dir, filename)
		with open(filepath, 'w') as f:
			f.write(instance.payload)

		# update filepath
		instance.filepath = filepath

	else:
		logger.debug('currently only xslt style transformations accepted')



##################################
# Apahce Livy
##################################

class LivyClient(object):

	'''
	Client used for HTTP requests made to Livy server.
	On init, pull Livy information and credentials from localsettings.py.
	
	This Class uses a combination of raw HTTP requests to Livy server, and the built-in
	python-api HttpClient.
		- raw requests are helpful for starting sessions, and getting session status
		- HttpClient useful for submitting jobs, closing session

	Sets class attributes from Django settings
	'''

	server_host = settings.LIVY_HOST 
	server_port = settings.LIVY_PORT 
	default_session_config = settings.LIVY_DEFAULT_SESSION_CONFIG


	@classmethod
	def http_request(self, http_method, url, data=None, headers={'Content-Type':'application/json'}, files=None, stream=False):

		'''
		Make HTTP request to Livy serer.

		Args:
			verb (str): HTTP verb to use for request, e.g. POST, GET, etc.
			url (str): expecting path only, as host is provided by settings
			data (str,file): payload of data to send for request
			headers (dict): optional dictionary of headers passed directly to requests.request, defaults to JSON content-type request
			files (dict): optional dictionary of files passed directly to requests.request
			stream (bool): passed directly to requests.request for stream parameter
		'''

		# prepare data as JSON string
		if type(data) != str:
			data = json.dumps(data)

		# build request
		session = requests.Session()
		request = requests.Request(http_method, "http://%s:%s/%s" % (self.server_host, self.server_port, url.lstrip('/')), data=data, headers=headers, files=files)
		prepped_request = request.prepare() # or, with session, session.prepare_request(request)
		response = session.send(
			prepped_request,
			stream=stream,
		)
		return response


	@classmethod
	def get_sessions(self):

		'''
		Return current Livy sessions

		Returns:
			Livy server response (dict)
		'''

		livy_sessions = self.http_request('GET','sessions')
		return livy_sessions


	@classmethod
	def create_session(self, config=None):

		'''
		Initialize Livy/Spark session.

		Args:
			config (dict): optional configuration for Livy session, defaults to settings.LIVY_DEFAULT_SESSION_CONFIG

		Returns:
			Livy server response (dict)
		'''

		# if optional session config provided, use, otherwise use default session config from localsettings
		if config:
			data = config
		else:
			data = self.default_session_config

		# issue POST request to create new Livy session
		return self.http_request('POST', 'sessions', data=data)


	@classmethod
	def session_status(self, session_id):

		'''
		Return status of Livy session based on session id

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		return self.http_request('GET','sessions/%s' % session_id)


	@classmethod
	def stop_session(self, session_id):

		'''
		Assume session id's are unique, change state of session DB based on session id only
			- as opposed to passing session row, which while convenient, would limit this method to 
			only stopping sessions with a LivySession row in the DB

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		# remove session
		return self.http_request('DELETE','sessions/%s' % session_id)


	@classmethod
	def get_jobs(self, session_id, python_code):

		'''
		Get all jobs (statements) for a session

		Args:
			session_id (str/int): Livy session id

		Returns:
			Livy server response (dict)
		'''

		# statement
		jobs = self.http_request('GET', 'sessions/%s/statements' % session_id)
		return job


	@classmethod
	def job_status(self, job_url):

		'''
		Get status of job (statement) for a session

		Args:
			job_url (str/int): full URL for statement in Livy session

		Returns:
			Livy server response (dict)
		'''

		# statement
		statement = self.http_request('GET', job_url)
		return statement


	@classmethod
	def submit_job(self, session_id, python_code):

		'''
		Submit job via HTTP request to /statements

		Args:
			session_id (str/int): Livy session id
			python_code (str): 

		Returns:
			Livy server response (dict)
		'''

		logger.debug(python_code)
		
		# statement
		job = self.http_request('POST', 'sessions/%s/statements' % session_id, data=json.dumps(python_code))
		logger.debug(job.json())
		logger.debug(job.headers)
		return job


	@classmethod
	def stop_job(self, job_url):

		'''
		Stop job via HTTP request to /statements

		Args:
			job_url (str/int): full URL for statement in Livy session

		Returns:
			Livy server response (dict)
		'''

		# statement
		statement = self.http_request('POST', '%s/cancel' % job_url)
		return statement
		


##################################
# Combine Models
##################################

class PublishedRecords(object):

	'''
	Simple container for all jobs
	'''

	def __init__(self):

		self.record_group = 0

		# get published jobs
		self.publish_links = JobPublish.objects.all()

		# get set IDs from record group of published jobs
		self.sets = { publish_link.record_group.publish_set_id:publish_link.job for publish_link in self.publish_links }

		# get iterable queryset of records
		self.records = Record.objects.filter(job__job_type = 'PublishJob')

		# set record count
		self.record_count = self.records.count()


	def get_record(self, id):

		'''
		Return single, published record by id
		'''

		record_query = records.filter(record_id = id)

		# if one, return
		if record_query.count() == 1:
			return record_query.first()

		else:
			raise Exception('multiple records found for id %s - this is not allowed for published records' % id)



class CombineJob(object):


	def __init__(self, user=None, job_id=None, parse_job_output=True):

		self.user = user
		self.livy_session = self._get_active_livy_session()
		self.df = None
		self.job_id = job_id

		# if job_id provided, attempt to retrieve and parse output
		if self.job_id:

			# retrieve job
			self.get_job(self.job_id)


	def default_job_name(self):

		'''
		provide default job name based on class type and date
		'''

		return '%s @ %s' % (type(self).__name__, datetime.datetime.now().isoformat())


	@staticmethod
	def get_combine_job(job_id):

		# get job from db
		j = Job.objects.get(pk=job_id)

		# using job_type, return instance of approriate job type
		return globals()[j.job_type](job_id=job_id)


	def _get_active_livy_session(self):

		'''
		Method to retrieve active livy session
		'''

		# check for single, active livy session from LivyClient
		livy_sessions = LivySession.objects.filter(active=True)

		# if single session, confirm active or starting
		if livy_sessions.count() == 1:
			
			livy_session = livy_sessions.first()
			logger.debug('single livy session found, confirming running')

			try:
				livy_session_status = LivyClient().session_status(livy_session.session_id)
				if livy_session_status.status_code == 200:
					status = livy_session_status.json()['state']
					if status in ['starting','idle','busy']:
						# return livy session
						return livy_session
					
			except:
				logger.debug('could not confirm session status')

		elif livy_sessions.count() == 0:
			logger.debug('no active livy sessions found')
			return False


	def start_job(self):

		'''
		starts job, sends to prepare_job() for child classes
		'''

		# if active livy session
		if self.livy_session:
			self.prepare_job()

		else:
			logger.debug('could not submit livy job, not active livy session found')
			return False


	def submit_job_to_livy(self, job_code, job_output):

		# submit job
		submit = LivyClient().submit_job(self.livy_session.session_id, job_code)
		response = submit.json()
		headers = submit.headers

		# update job in DB
		self.job.spark_code = job_code
		self.job.job_id = int(response['id'])
		self.job.status = response['state']
		self.job.url = headers['Location']
		self.job.headers = headers
		self.job.save()


	def get_job(self, job_id):

		'''
		Retrieve job information from DB to perform other tasks

		Args:
			job_id (int): Job ID
		'''

		self.job = Job.objects.filter(id=job_id).first()


	def count_records(self):

		'''
		Use methods from models.Job
		'''

		return self.job.get_records().count()


	def get_record(self, id):

		'''
		Convenience method to return single record from job
		'''

		record_query = Record.objects.filter(job=self.job).filter(record_id=id)

		# if only one found
		if record_query.count() == 1:
			return record_query.first()

		# else, return all results
		else:
			return record_query


	def count_indexed_fields(self):

		'''
		Count instances of fields across all documents in a job's index, if exists
		'''

		if es_handle.indices.exists(index='j%s' % self.job_id):

			# get mappings for job index
			es_r = es_handle.indices.get(index='j%s' % self.job_id)
			index_mappings = es_r['j%s' % self.job_id]['mappings']['record']['properties']

			# sort alphabetically that influences results list
			field_names = list(index_mappings.keys())
			field_names.sort()

			# init search
			s = Search(using=es_handle, index='j%s' % self.job_id)

			# return no results, only aggs
			s = s[0]

			# add agg buckets for each field to count total and unique instances
			for field_name in field_names:
				s.aggs.bucket('%s_instances' % field_name, A('filter', Q('exists', field=field_name)))
				s.aggs.bucket('%s_distinct' % field_name, A('cardinality', field='%s.keyword' % field_name))

			# execute search and capture as dictionary
			sr = s.execute()
			sr_dict = sr.to_dict()

			# calc fields percentage and return as list
			field_count = [ 
				{
					'field_name':field,
					'instances':sr_dict['aggregations']['%s_instances' % field]['doc_count'],
					'distinct':sr_dict['aggregations']['%s_distinct' % field]['value'],
					'distinct_ratio':round((sr_dict['aggregations']['%s_distinct' % field]['value'] / sr_dict['aggregations']['%s_instances' % field]['doc_count']), 4),
					'percentage_of_total_records':round((sr_dict['aggregations']['%s_instances' % field]['doc_count'] / sr_dict['hits']['total']), 4)
				}
				for field in field_names
			]

			# return
			return {
				'total_docs':sr_dict['hits']['total'],
				'fields':field_count
			}

		else:

			return False


	def field_analysis(self, field_name):

		'''
		For a given field, return all values for that field across a job's index
		'''

		# init search
		s = Search(using=es_handle, index='j%s' % self.job_id)

		# add agg bucket for field values
		s.aggs.bucket(field_name, A('terms', field='%s.keyword' % field_name, size=1000000))

		# return zero
		s = s[0]

		# execute and return aggs
		sr = s.execute()
		return sr.aggs[field_name]['buckets']


	def get_indexing_failures(self):

		'''
		return failures for job indexing process
		'''

		# load indexing failures for this job from DB
		index_failures = IndexMappingFailure.objects.filter(job=self.job)
		return index_failures


	def get_total_input_job_record_count(self):

		'''
		return record count sum from all input jobs
		'''

		if self.job.jobinput_set.count() > 0:
			total_input_record_count = sum([ input_job.input_job.record_count for input_job in self.job.jobinput_set.all() ])
			return total_input_record_count
		else:
			return None


	def get_job_output_filename_hash(self):

		'''
		return hash of avro filenames
		'''

		# get list of avro files
		job_output_dir = self.job.job_output.split('file://')[-1]

		try:
			avros = [f for f in os.listdir(job_output_dir) if f.endswith('.avro')]

			if len(avros) > 0:
				job_output_filename_hash = re.match(r'part-r-[0-9]+-(.+?)\.avro', avros[0]).group(1)
				logger.debug('job output filename hash: %s' % job_output_filename_hash)
				return job_output_filename_hash

			elif len(avros) == 0:
				logger.debug('no avro files found in job output directory')
				return False
		except:
			logger.debug('could not load job output to determine filename hash')
			return False
		


class HarvestJob(CombineJob):


	def __init__(self,
		job_name=None,
		user=None,
		record_group=None,
		oai_endpoint=None,
		overrides=None,
		job_id=None,
		index_mapper=None):

		'''
		Harvest from OAI-PMH endpoint.

		Unlike other jobs, harvests do not require input from the output of another job

		Args:
			user (User or core.models.CombineUser): user that will issue job
			record_group (core.models.RecordGroup): record group instance that will be used for harvest
			oai_endpoint (core.models.OAIEndpoint): OAI endpoint to be used for OAI harvest
			overrides (dict): optional dictionary of overrides to OAI endpoint

		Returns:

			avro file set:
				- record
				- error
				- setIds
		'''

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.record_group = record_group		
			self.organization = self.record_group.organization
			self.oai_endpoint = oai_endpoint
			self.overrides = overrides
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB and save
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None
			)
			self.job.save()


	def prepare_job(self):

		'''
		Construct python code that will be sent to Livy for harvest job
		'''

		# create shallow copy of oai_endpoint and mix in overrides
		harvest_vars = self.oai_endpoint.__dict__.copy()
		harvest_vars.update(self.overrides)

		# prepare job code
		job_code = {
			'code':'from jobs import HarvestSpark\nHarvestSpark.spark_function(spark, endpoint="%(endpoint)s", verb="%(verb)s", metadataPrefix="%(metadataPrefix)s", scope_type="%(scope_type)s", scope_value="%(scope_value)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'endpoint':harvest_vars['endpoint'],
				'verb':harvest_vars['verb'],
				'metadataPrefix':harvest_vars['metadataPrefix'],
				'scope_type':harvest_vars['scope_type'],
				'scope_value':harvest_vars['scope_value'],
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		return harvest job specific errors
		REVISIT: Currently, we are not saving errors from OAI harveset, and so, cannot retrieve...
		'''

		return None



class TransformJob(CombineJob):
	
	'''
	Apply an XSLT transformation to a record group
	'''

	def __init__(self,
		job_name=None,
		user=None,
		record_group=None,
		input_job=None,
		transformation=None,
		job_id=None,
		index_mapper=None):

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_job = input_job
			self.transformation = transformation
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None,
				job_details = json.dumps(
					{'transformation':
						{
							'name':self.transformation.name,
							'type':self.transformation.transformation_type,
							'id':self.transformation.id
						}
					})
			)
			self.job.save()

			# save input job to JobInput table
			job_input_link = JobInput(job=self.job, input_job=self.input_job)
			job_input_link.save()


	def prepare_job(self):

		'''
		Construct python code that will be sent to Livy for transform job
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import TransformSpark\nTransformSpark.spark_function(spark, transform_filepath="%(transform_filepath)s", job_input="%(job_input)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'transform_filepath':self.transformation.filepath,
				'job_input':self.input_job.job_output,
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		'''
		return transform job specific errors
		'''

		return self.job.get_errors()



class MergeJob(CombineJob):
	
	'''
	Merge multiple jobs into a single job
	'''

	def __init__(self,
		job_name=None,
		user=None,
		record_group=None,
		input_jobs=None,
		job_id=None,
		index_mapper=None):

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_jobs = input_jobs
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None,
				job_details = json.dumps(
					{'publish':
						{
							'publish_job_id':str(self.input_jobs),
						}
					})
			)
			self.job.save()

			# save input job to JobInput table
			for input_job in self.input_jobs:
				job_input_link = JobInput(job=self.job, input_job=input_job)
				job_input_link.save()


	def prepare_job(self):

		'''
		Construct python code that will be sent to Livy for publish job
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import MergeSpark\nMergeSpark.spark_function(spark, sc, job_inputs="%(job_inputs)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'job_inputs':str([ input_job.job_output for input_job in self.input_jobs ]),
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		pass



class PublishJob(CombineJob):
	
	'''
	Copy record output from job as published job set
	'''

	def __init__(self,
		job_name=None,
		user=None,
		record_group=None,
		input_job=None,
		job_id=None,
		index_mapper=None):

		# perform CombineJob initialization
		super().__init__(user=user, job_id=job_id)

		# if job_id not provided, assumed new Job
		if not job_id:

			self.job_name = job_name
			self.record_group = record_group
			self.organization = self.record_group.organization
			self.input_job = input_job
			self.index_mapper = index_mapper

			# if job name not provided, provide default
			if not self.job_name:
				self.job_name = self.default_job_name()

			# create Job entry in DB
			self.job = Job(
				record_group = self.record_group,
				job_type = type(self).__name__,
				user = self.user,
				name = self.job_name,
				spark_code = None,
				job_id = None,
				status = 'initializing',
				url = None,
				headers = None,
				job_output = None,
				job_details = json.dumps(
					{'publish':
						{
							'publish_job_id':self.input_job.id,
						}
					})
			)
			self.job.save()

			# save input job to JobInput table
			job_input_link = JobInput(job=self.job, input_job=self.input_job)
			job_input_link.save()

			# save publishing link from job to record_group
			job_publish_link = JobPublish(record_group=self.record_group, job=self.job)
			job_publish_link.save()


	def prepare_job(self):

		'''
		Construct python code that will be sent to Livy for publish job
		'''

		# prepare job code
		job_code = {
			'code':'from jobs import PublishSpark\nPublishSpark.spark_function(spark, job_input="%(job_input)s", job_id="%(job_id)s", index_mapper="%(index_mapper)s")' % 
			{
				'job_input':self.input_job.job_output,
				'job_id':self.job.id,
				'index_mapper':self.index_mapper
			}
		}
		logger.debug(job_code)

		# submit job
		self.submit_job_to_livy(job_code, self.job.job_output)


	def get_job_errors(self):

		pass




