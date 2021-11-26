import json
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class CCSparkJob:
    """
    A simple Spark job definition to process Common Crawl data
    """

    name = 'CCSparkJob'

    warc_parse_http_header = True

    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    

    def __init__(self, input_file_listing=None, num_input_partitions=32, local_temp_dir=None, 
                 log_level='INFO', spark_profiler=None, spark_master_url=None):

        # Path to file listing input paths
        self.input_file_listing = input_file_listing
        # Number of input splits/partitions, number of parallel tasks to process WARC files/records
        self.num_input_partitions = num_input_partitions
        # Local temporary directory, used to buffer content from S3
        self.local_temp_dir = local_temp_dir
        # Logging level
        self.log_level = log_level
        # Enable PySpark profiler and log profiling metrics if job has finished, cf. spark.python.profile
        self.spark_profiler = spark_profiler
        # address of spark master node
        self.spark_master_url = spark_master_url

        logging.basicConfig(level=self.log_level, format=LOGGING_FORMAT)


    def get_output_options(self):
        return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                        self.output_option)}

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def init_accumulators(self, sc):
        self.records_processed = sc.accumulator(0)
        self.warc_input_processed = sc.accumulator(0)
        self.warc_input_failed = sc.accumulator(0)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)

    def run(self, url_only):
        conf = SparkConf()

        if self.spark_profiler:
            conf = conf.set("spark.python.profile", "true")

        # add packages to allow pulling from AWS S3
        conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk:1.11.375')

        # anon creds for aws
        conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
        conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

        if self.spark_master_url:

            # set spark container image
            conf.set('spark.kubernetes.container.image', 'datamechanics/spark:3.2.0-latest')

            # allow spark worker scaling
            dynamic_scaling = False
            if dynamic_scaling:
                conf.set('spark.dynamicAllocation.enabled', 'true')
                conf.set('spark.dynamicAllocation.shuffleTracking.enabled', 'true')
                conf.set("spark.dynamicAllocation.maxExecutors","5")
            else:
                conf.set('spark.executor.instances', '1')

            # specify pod size
            conf.set('spark.executor.cores', '32')
            conf.set('spark.kubernetes.executor.request.cores', '28800m')
            conf.set('spark.executor.memory', '450g')

            # add labels to pods
            conf.set('spark.kubernetes.executor.label.app', 'seldonite')

            # allow python deps to be used
            this_dir_path = os.path.dirname(os.path.abspath(__file__))
            conda_package_path = os.path.join(this_dir_path, 'pyspark_conda_env.tar.gz')
            os.environ['PYSPARK_PYTHON'] = './environment/bin/python'
            conf.set('spark.archives', f'{conda_package_path}#environment')

            sc = SparkContext(
                master=self.spark_master_url,
                appName=self.name,
                conf=conf
            )
        else:
            os.environ['PYSPARK_PYTHON'] = 'python'
            sc = SparkContext(
                appName=self.name,
                conf=conf
            )
        
        sqlc = SQLContext(sparkContext=sc)

        self.init_accumulators(sc)

        result = self.run_job(sc, sqlc, url_only)

        if self.spark_profiler:
            sc.show_profiles()

        sc.stop()

        return result

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.warc_input_processed,
                            'WARC/WAT/WET input files processed = {}')
        self.log_aggregator(sc, self.warc_input_failed,
                            'WARC/WAT/WET input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'WARC/WAT/WET records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    def run_job(self, sc, sqlc):
        input_data = sc.textFile(self.input_file_listing,
                                 minPartitions=self.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_warcs) \
            .reduceByKey(self.reduce_by_key_func)

        sqlc.createDataFrame(output, schema=self.output_schema) \
            .coalesce(self.num_output_partitions) \
            .write \
            .format(self.output_format) \
            .option("compression", self.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.output)

        self.log_aggregators(sc)

    def process_warcs(self, id_, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        for uri in iterator:
            self.warc_input_processed.add(1)
            if not uri.startswith('s3://'):
                raise ValueError('Cannot parse not S3 files with this implementation')

            self.get_logger().info('Reading from S3 {}'.format(uri))
            s3match = s3pattern.match(uri)
            if s3match is None:
                self.get_logger().error("Invalid S3 URI: " + uri)
                continue
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b',
                                        dir=self.local_temp_dir)
            try:
                s3client.download_fileobj(bucketname, path, warctemp)
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to download {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                warctemp.close()
                continue
            warctemp.seek(0)
            stream = warctemp

            no_parse = (not self.warc_parse_http_header)
            try:
                archive_iterator = ArchiveIterator(stream,
                                                   no_record_parse=no_parse, arc2warc=True)
                for res in self.iterate_records(uri, archive_iterator):
                    yield res
            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC: {} - {}'.format(uri, exception))
            finally:
                stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, _warc_uri, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for record in archive_iterator:
            yield self.process_record(record)
            self.records_processed.add(1)
            # WARC record offset and length should be read after the record
            # has been processed, otherwise the record content is consumed
            # while offset and length are determined:
            #  warc_record_offset = archive_iterator.get_record_offset()
            #  warc_record_length = archive_iterator.get_record_length()

    @staticmethod
    def is_wet_text_record(record):
        """Return true if WARC record is a WET text/plain record"""
        return (record.rec_type == 'conversion' and
                record.content_type == 'text/plain')

    @staticmethod
    def is_wat_json_record(record):
        """Return true if WARC record is a WAT record"""
        return (record.rec_type == 'metadata' and
                record.content_type == 'application/json')

    @staticmethod
    def is_html(record):
        """Return true if (detected) MIME type of a record is HTML"""
        html_types = ['text/html', 'application/xhtml+xml']
        if (('WARC-Identified-Payload-Type' in record.rec_headers) and
            (record.rec_headers['WARC-Identified-Payload-Type'] in
             html_types)):
            return True
        content_type = record.http_headers.get_header('content-type', None)
        if content_type:
            for html_type in html_types:
                if html_type in content_type:
                    return True
        return False


class CCIndexSparkJob(CCSparkJob):
    """
    Process the Common Crawl columnar URL index
    """

    name = "CCIndexSparkJob"

    # description of input and output shown in --help
    input_descr = "Path to Common Crawl index table"

    def __init__(self, table_path='s3a://commoncrawl/cc-index/table/cc-main/warc/', table_name='ccindex', query="SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE crawl = 'CC-MAIN-2020-24' AND subset = 'warc' LIMIT 10", **kwargs):
        super().__init__(**kwargs)
        # Name of the table data is loaded into
        self.table_name = table_name
        # SQL query to select rows (required)
        self.query = query
        # JSON schema of the ccindex table, implied from Parquet files if not provided.
        table_schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cc-index-schema-flat.json')
        
        with open(table_schema_path, 'r') as s:
            self.table_schema = StructType.fromJson(json.loads(s.read()))
        # path to default common crawl index
        self.table_path = table_path

    def load_table(self, sc, spark):
        parquet_reader = spark.read.format('parquet')
        parquet_reader = parquet_reader.schema(self.table_schema)
        df = parquet_reader.load(self.table_path)
        df.createOrReplaceTempView(self.table_name)
        self.get_logger(sc).info(
            "Schema of table {}:\n{}".format(self.table_name, df.schema))

    def execute_query(self, sc, spark, query):
        sqldf = spark.sql(query)
        self.get_logger(sc).info("Executing query: {}".format(query))
        sqldf.explain()
        return sqldf

    def load_dataframe(self, sc, partitions=-1):
        session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
        if self.query is not None:
            self.load_table(sc, session)
            sqldf = self.execute_query(sc, session, self.query)
        else:
            sqldf = session.read.format("csv").option("header", True) \
                .option("inferSchema", True).load(self.csv)
        sqldf.persist()

        num_rows = sqldf.count()
        self.get_logger(sc).info(
            "Number of records/rows matched by query: {}".format(num_rows))

        if partitions > 0:
            self.get_logger(sc).info(
                "Repartitioning data to {} partitions".format(partitions))
            sqldf = sqldf.repartition(partitions)

        return sqldf

    def run_job(self, sc, sqlc):
        sqldf = self.load_dataframe(sc, self.num_output_partitions)

        sqldf.write \
            .format(self.output_format) \
            .option("compression", self.output_compression) \
            .options(**self.get_output_options()) \
            .saveAsTable(self.output)

        self.log_aggregators(sc)


class CCIndexWarcSparkJob(CCIndexSparkJob):
    """
    Process Common Crawl data (WARC records) found by the columnar URL index
    """

    name = "CCIndexWarcSparkJob"

    def __init__(self, query=None, csv=None, **kwargs):
        super().__init__(**kwargs)
        # SQL query to select rows. 
        # Note: the result is required to contain the columns `url', `warc_filename', `warc_record_offset' and `warc_record_length', make sure they're SELECTed. 
        # The column `content_charset' is optional and is utilized to read WARC record payloads with the right encoding.
        if query is not None:
            self.query = query
        # CSV file to load WARC records by filename, offset and length. 
        # The CSV file must have column headers 
        # and the input columns `url', `warc_filename', `warc_record_offset' and `warc_record_length' are mandatory, see also option query.
        self.csv = csv

    def fetch_process_warc_records(self, rows):
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)
        bucketname = "commoncrawl"
        no_parse = (not self.warc_parse_http_header)


        # TODO check for content truncated
        for row in rows:
            url = row['url']
            warc_path = row['warc_filename']
            offset = int(row['warc_record_offset'])
            length = int(row['warc_record_length'])
            content_charset = None
            if 'content_charset' in row:
                content_charset = row['content_charset']
            self.get_logger().debug("Fetching WARC record for {}".format(url))
            rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
            try:
                response = s3client.get_object(Bucket=bucketname,
                                               Key=warc_path,
                                               Range=rangereq)
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to download: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))
                self.warc_input_failed.add(1)
                continue
            record_stream = BytesIO(response["Body"].read())
            try:
                for record in ArchiveIterator(record_stream,
                                              no_record_parse=no_parse):
                    # pass `content_charset` forward to subclass processing WARC records
                    record.rec_headers['WARC-Identified-Content-Charset'] = content_charset
                    success, article = self.process_record(url, record)
                    if success:
                        yield article

                    self.records_processed.add(1)

            except ArchiveLoadFailed as exception:
                self.warc_input_failed.add(1)
                self.get_logger().error(
                    'Invalid WARC record: {} ({}, offset: {}, length: {}) - {}'
                    .format(url, warc_path, offset, length, exception))

    def run_job(self, sc, sqlc, url_only):
        sqldf = self.load_dataframe(sc, self.num_input_partitions)

        if url_only:
            columns = ['url']
            warc_recs = sqldf.select(*columns).rdd
            return warc_recs.flatMap(lambda x: x).collect()
        else:
            columns = ['url', 'warc_filename', 'warc_record_offset', 'warc_record_length']
            if 'content_charset' in sqldf.columns:
                columns.append('content_charset')
            warc_recs = sqldf.select(*columns).rdd

        num_warcs = warc_recs.count()
        if num_warcs == 0:
            raise ValueError()

        rdd = warc_recs.mapPartitions(self.fetch_process_warc_records)
        output = rdd.collect()

        self.log_aggregators(sc)

        return output
        # sqlc.createDataFrame(output, schema=self.output_schema) \
        #     .coalesce(self.num_output_partitions) \
        #     .write \
        #     .format(self.output_format) \
        #     .option("compression", self.output_compression) \
        #     .options(**self.get_output_options()) \
        #     .saveAsTable(self.output)
