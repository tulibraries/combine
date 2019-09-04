from django.conf import settings
import os


# Deployment type
COMBINE_DEPLOYMENT = os.environ.get('COMBINE_DEPLOYMENT', 'docker')


# Combine Install Location
COMBINE_INSTALL_PATH = os.environ.get('COMBINE_INSTALL_PATH', '/opt/combine')


# Combine Front-End
APP_HOST = os.environ.get('APP_HOST', '10.5.0.10')


# Spark Cluster Information
# Note: configured to use Livy running in local[*] mode
SPARK_HOST = os.environ.get('SPARK_HOST', '10.5.0.11')
SPARK_PORT = os.environ.get('SPARK_PORT', 8080)
SPARK_APPLICATION_ROOT_PORT = 4040 # if taken, will automatically increment +100 from here until open port is found


# Spark tuning
SPARK_MAX_WORKERS = os.environ.get('SPARK_MAX_WORKERS', 1)
JDBC_NUMPARTITIONS = os.environ.get('JDBC_NUMPARTITIONS', 200)
SPARK_REPARTITION = os.environ.get('SPARK_REPARTITION', 200)
TARGET_RECORDS_PER_PARTITION = os.environ.get('TARGET_RECORDS_PER_PARTITION', 5000)
MONGO_READ_PARTITION_SIZE_MB = os.environ.get('MONGO_READ_PARTITION_SIZE_MB', 4)


# Apache Livy settings
'''
Combine uses Livy to issue spark statements.
Livy provides a stateless pattern for interacting with Spark, and by proxy, DPLA code.
'''
LIVY_HOST = os.environ.get('LIVY_HOST', '10.5.0.11')
LIVY_PORT = os.environ.get('LIVY_PORT', 8998)
LIVY_DEFAULT_SESSION_CONFIG = {
    'kind':'pyspark',
    'jars':[
    	'file:///combinelib/mysql.jar'
    ],
    'files':[
    	'file://%s/core/spark/es.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    	'file://%s/core/spark/jobs.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    	'file://%s/core/spark/record_validation.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    	'file://%s/core/spark/utils.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    	'file://%s/core/spark/console.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    	'file://%s/core/xml2kvp.py' % COMBINE_INSTALL_PATH.rstrip('/'),
    ],

    # Spark conf overrides
	'conf':{
		'spark.ui.port':SPARK_APPLICATION_ROOT_PORT
	},

    # Spark application overrides
		# commented out, will default to 2gb for driver and executor, and grab all available cpu/cores
		# uncommented, provides some limited configurations for tuning Spark application created by Livy

	# e.g. small(ish) server, 4gb RAM, 2 cpu/cores
	# 'driverMemory':'512m',
	# 'driverCores':1,
	# 'executorMemory':'512m',
	# 'executorCores':1,
	# 'numExecutors':1
}


# Storage for avro files and other binary files
'''
Make sure to note file:// or hdfs:// prefix
'''
BINARY_STORAGE = os.environ.get('BINARY_STORAGE', 'file:///home/combine/data/combine')
WRITE_AVRO = os.environ.get('WRITE_AVRO', False)


# ElasicSearch server
ES_HOST = os.environ.get('ES_HOST', '10.5.0.2')
INDEX_TO_ES = os.environ.get('INDEX_TO_ES', True)


# ElasticSearch analysis
CARDINALITY_PRECISION_THRESHOLD = os.environ.get('CARDINALITY_PRECISION_THRESHOLD', 100)
ONE_PER_DOC_OFFSET = os.environ.get('ONE_PER_DOC_OFFSET', 0.05)


# Service Hub
SERVICE_HUB_PREFIX = os.environ.get('SERVICE_HUB_PREFIX', 'foo--')


# OAI Server
OAI_RESPONSE_SIZE = os.environ.get('OAI_RESPONSE_SIZE', 500)
COMBINE_OAI_IDENTIFIER = os.environ.get('COMBINE_OAI_IDENTIFIER', 'oai:funnel_cake')
METADATA_PREFIXES = {
	'mods':{
			'schema':'http://www.loc.gov/standards/mods/v3/mods.xsd',
			'namespace':'http://www.loc.gov/mods/v3'
		},
	'oai_dc':{
			'schema':'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
			'namespace':'http://purl.org/dc/elements/1.1/'
		},
	'dc':{
			'schema':'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
			'namespace':'http://purl.org/dc/elements/1.1/'
		},
}


# Database configurations for use in Spark context
COMBINE_DATABASE = {
	'jdbc_url':'jdbc:mysql://%s:3306/combine' % '10.5.0.4',
	'user':settings.DATABASES['default']['USER'],
	'password':settings.DATABASES['default']['PASSWORD']
}

# DPLA API
DPLA_RECORD_MATCH_QUERY = os.environ.get('DPLA_RECORD_MATCH_QUERY', True)
DPLA_API_KEY = os.environ.get('DPLA_API_KEY', None)


# AWS S3 Credentials
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', None)
DPLA_S3_BUCKET = os.environ.get('DPLA_S3_BUCKET', 'dpla-provider-export')


# Analysis Jobs Org and Record Group
'''
This dictionary provides the name of the Organization and Record Group that Analysis Jobs will be created under.
Because Analysis jobs are extremely similar to other workflow jobs, but do not lend themselves towards the established
Organization --> Record Group --> Job hierarchy, this ensures they are treated similarily to other jobs, but skip the
need for users to manually create these somewhat unique Organization and Record Group.
	- it is recommended to make these names quite unique, to avoid clashing with user created Orgs and Record Groups
	- the Organization and Record Group names defined in ANALYSIS_JOBS_HIERARCHY will NOT show up in any Org or Record
	Group views or other workflows
	- it is quite normal, and perhaps even encouraged, to leave these as the defaults provided
'''
ANALYSIS_JOBS_HIERARCHY = {
	'organization':'AnalysisOrganizationf8ed4bfcefc4dbf87b588a5de9b7cc95', # suffix is md5 hash of 'AnalysisOrganization'
	'record_group':'AnalysisRecordGroupf660bb4826bea8b63fd773d27d687cfd' # suffix is md5 hash of 'AnalysisRecordGroup'
}

# Celery Configurations
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://10.5.0.5:6379/0')
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', 'redis://10.5.0.5:6379/0')


# StateIO Configurations
'''
Configurations used for exporting/importing "states" in Combine, including Organizations, Record Groups,
Jobs, Validation Scenarios, Transformation Scenarios, etc.  These can be large in size, and potentially helpful
to preserve, so /tmp is not ideal here.
'''
STATEIO_EXPORT_DIR = os.environ.get('STATEIO_EXPORT_DIR', '/home/combine/data/combine/stateio/exports')
STATEIO_IMPORT_DIR = os.environ.get('STATEIO_IMPORT_DIR', '/home/combine/data/combine/stateio/imports')


# Mongo server
MONGO_HOST = os.environ.get('MONGO_HOST', '10.5.0.3')


# Docker override for DATABASE settings
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': 'combine',
        'USER': 'combine',
        'PASSWORD': 'combine',
        'HOST': '10.5.0.4',
        'PORT': '3306',
    }
}
