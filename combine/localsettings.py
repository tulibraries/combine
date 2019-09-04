"""Local settings for use within Django app. Requires reload / rebuild to pick up for deploy."""
import os
from django.conf import settings

# Default Settings Overrides
SECRET_KEY = os.getenv('APP_SECRET_KEY', 'blergh')
DEBUG = os.getenv('APP_DEBUG_MODE', False)
ALLOWED_HOSTS = [
    'localhost',
    '127.0.0.1',
    os.getenv('APP_HOST'),
    os.getenv('APP_FQDN')
]

# Deployment type
COMBINE_DEPLOYMENT = os.getenv('COMBINE_DEPLOYMENT', 'server')

# Combine Install Location
COMBINE_INSTALL_PATH = os.getenv('COMBINE_INSTALL_PATH', '/opt/combine')

# Combine Front-End
APP_HOST = os.getenv('APP_HOST', '127.0.0.1')

# Spark Cluster Information
SPARK_HOST = os.getenv('SPARK_HOST', '127.0.0.1')
SPARK_PORT = os.getenv('SPARK_PORT', 8080)
# if taken, will automatically increment +100 from here until open port is found
SPARK_APPLICATION_ROOT_PORT = 4040

# Spark tuning
SPARK_MAX_WORKERS = os.getenv('SPARK_MAX_WORKERS', 1)
JDBC_NUMPARTITIONS = os.getenv('JDBC_NUMPARTITIONS', 200)
SPARK_REPARTITION = os.getenv('SPARK_REPARTITION', 200)
TARGET_RECORDS_PER_PARTITION = os.getenv('TARGET_RECORDS_PER_PARTITION', 5000)
MONGO_READ_PARTITION_SIZE_MB = os.getenv('MONGO_READ_PARTITION_SIZE_MB', 4)

# Apache Livy settings
'''
Combine uses Livy to issue spark statements.
Livy provides a stateless pattern for interacting with Spark, and by proxy, DPLA code.
'''
LIVY_HOST = os.getenv('LIVY_HOST', '127.0.0.1')
LIVY_PORT = os.getenv('LIVY_PORT', 8998)
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
        'spark.ui.port': SPARK_APPLICATION_ROOT_PORT
    },
}

# Storage for avro files and other binary files
'''
Make sure to note file:// or hdfs:// prefix
'''
BINARY_STORAGE = os.getenv('BINARY_STORAGE', 'file:///home/combine/data/combine')
WRITE_AVRO = os.getenv('WRITE_AVRO', False)

# ElasicSearch server
ES_HOST = os.getenv('ES_HOST', '127.0.0.1')
INDEX_TO_ES = os.getenv('INDEX_TO_ES', True)

# ElasticSearch analysis
CARDINALITY_PRECISION_THRESHOLD = os.getenv('CARDINALITY_PRECISION_THRESHOLD', 100)
ONE_PER_DOC_OFFSET = os.getenv('ONE_PER_DOC_OFFSET', 0.05)

# Service Hub
SERVICE_HUB_PREFIX = os.getenv('SERVICE_HUB_PREFIX', 'funcake--')

# OAI Server
OAI_RESPONSE_SIZE = os.getenv('OAI_RESPONSE_SIZE', 500)
COMBINE_OAI_IDENTIFIER = os.getenv('COMBINE_OAI_IDENTIFIER', 'oai:funnel_cake')
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
    'jdbc_url':os.getenv('MYSQL_JDBC', 'jdbc:mysql://%s:3306/combine' % settings.DATABASES['default']['HOST']),
    'user':os.getenv('MYSQL_USER', settings.DATABASES['default']['USER']),
    'password':os.getenv('MYSQL_PASSWORD', settings.DATABASES['default']['PASSWORD'])
}

# DPLA API
DPLA_RECORD_MATCH_QUERY = os.getenv('DPLA_RECORD_MATCH_QUERY', True)
DPLA_API_KEY = os.getenv('DPLA_API_KEY', None)

# AWS S3 Credentials
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)
DPLA_S3_BUCKET = os.getenv('DPLA_S3_BUCKET', 'dpla-provider-export')

# Analysis Jobs Org and Record Group
'''
This dictionary provides the name of the Organization and Record Group that
Analysis Jobs will be created under. Because Analysis jobs are extremely similar
to other workflow jobs, but do not lend themselves towards the established
Organization --> Record Group --> Job hierarchy, this ensures they are treated
similarily to other jobs, but skip the need for users to manually create these
somewhat unique Organization and Record Group.
    - it is recommended to make these names quite unique,
      to avoid clashing with user created Orgs and Record Groups
    - the Organization and Record Group names defined in ANALYSIS_JOBS_HIERARCHY
      will NOT show up in any Org or Record Group views or other workflows
	- it is quite normal, and perhaps encouraged, to leave these as the defaults
'''
ANALYSIS_JOBS_HIERARCHY = {
    # suffix is md5 hash of 'AnalysisOrganization'
    'organization':'AnalysisOrganizationf8ed4bfcefc4dbf87b588a5de9b7cc95',
    # suffix is md5 hash of 'AnalysisRecordGroup'
    'record_group':'AnalysisRecordGroupf660bb4826bea8b63fd773d27d687cfd'
}

# Celery Configurations
CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

# StateIO Configurations
'''
Configurations used for exporting/importing "states" in Combine, including
Organizations, Record Groups, Jobs, Validation Scenarios, Transformation
Scenarios, etc.  These can be large in size, and potentially helpful to
preserve, so /tmp is not ideal here.
'''
STATEIO_EXPORT_DIR = os.getenv('STATEIO_EXPORT_DIR', '/home/combine/data/combine/stateio/exports')
STATEIO_IMPORT_DIR = os.getenv('STATEIO_IMPORT_DIR', '/home/combine/data/combine/stateio/imports')

# Mongo server
MONGO_HOST = os.getenv('MONGO_HOST', '127.0.0.1')

# overrides for DATABASE settings
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',
        'NAME': os.getenv('MYSQL_NAME', 'combine'),
        'USER': os.getenv('MYSQL_USER', 'combine'),
        'PASSWORD': os.getenv('MYSQL_PASSWORD', 'combine'),
        'HOST': os.getenv('MYSQL_HOST', '127.0.0.1'),
        'PORT': os.getenv('MYSQL_PORT', '3306'),
    }
}
