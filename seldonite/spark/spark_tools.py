from contextlib import contextmanager
import os

from bigdl.orca import init_orca_context, stop_orca_context
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession


class SparkBuilder():
    def __init__(self, master, name='seldonite_app', archives=[], executor_cores=16, executor_memory='160g', num_executors=1, spark_conf={}):

        self.use_bigdl_flag = False
        # address of spark master node
        self.spark_master_url = master
        self.archives = archives
        self.packages = []
        self.conf = {}
        # add user set spark conf
        self.conf.update(spark_conf.items())

        self.conf['spark.app.name'] = name

        # specify pod size
        self.conf['spark.executor.cores'] = str(executor_cores)
        self.conf['spark.executor.memory'] = executor_memory

        if self.spark_master_url:

            # set spark container image
            self.conf['spark.kubernetes.container.image'] = 'bigdl-k8s-spark-3.1.2-hadoop-3.2.0:latest'

            # allow spark worker scaling
            dynamic_scaling = False
            if dynamic_scaling:
                self.conf['spark.dynamicAllocation.enabled'] = 'true'
                self.conf['spark.dynamicAllocation.shuffleTracking.enabled'] = 'true'
                self.conf["spark.dynamicAllocation.maxExecutors"] = "5"
            else:
                self.conf['spark.executor.instances'] = str(num_executors)

            # add labels to pods
            self.conf['spark.kubernetes.executor.label.app'] = 'seldonite'

            # allow python deps to be used
            this_dir_path = os.path.dirname(os.path.abspath(__file__))
            conda_package_path = os.path.join(this_dir_path, 'seldonite_spark_env.tar.gz')
            os.environ['PYSPARK_PYTHON'] = '/opt/spark/work-dir/environment/bin/python'
            
            self.archives.append(f'{conda_package_path}#environment')

        else:
            self.conf['spark.executor.instances'] = 1

            os.environ['PYSPARK_PYTHON'] = 'python'

    def use_bigdl(self):
        self.use_bigdl_flag = True

    def set_conf(self, key, value):
        self.conf[key] = value

    def add_package(self, package):
        self.packages.append(package)

    def add_archive(self, archive):
        self.archives.append(archive)

    @contextmanager
    def start_session(self):
        self.conf['spark.jars.packages'] = ','.join(self.packages)
        self.conf['spark.archives'] = ','.join(self.archives)

        if self.use_bigdl_flag:
            os.environ['BIGDL_CLASSPATH'] = '/opt/spark/work-dir/environment/lib/python3.7/site-packages/bigdl/share/dllib/lib/bigdl-dllib-spark_3.1.2-0.14.0-SNAPSHOT-jar-with-dependencies.jar:/opt/spark/work-dir/environment/lib/python3.7/site-packages/bigdl/share/orca/lib/bigdl-orca-spark_3.1.2-0.14.0-SNAPSHOT-jar-with-dependencies.jar'

        spark_manager = SparkManager(self.spark_master_url, self.use_bigdl_flag, self.conf)
        try:
            yield spark_manager
        finally:
            spark_manager.end()

class SparkManager():
    def __init__(self, spark_master_url, use_bigdl, conf):

        self.spark_master_url = spark_master_url
        self.use_bigdl = use_bigdl
        self.conf = conf

        if self.spark_master_url:
            if self.use_bigdl:
                sc = init_orca_context(cluster_mode='k8s-client',
                                       num_nodes=int(conf['spark.executor.instances']),
                                       cores=int(conf['spark.executor.cores']),
                                       memory=conf['spark.executor.memory'],
                                       master=self.spark_master_url, 
                                       container_image=conf['spark.kubernetes.container.image'],
                                       conf=conf)
            else:
                spark_conf = SparkConf()
                spark_conf.setAll(conf.items())
                sc = SparkContext(
                    master=self.spark_master_url,
                    conf=spark_conf
                )
        else:
            if self.use_bigdl:
                sc = init_orca_context(cluster_mode='local',
                                       conf=conf)
            else:
                spark_conf = SparkConf()
                spark_conf.setAll(conf.items())
                sc = SparkContext(
                    conf=spark_conf
                )

        self._spark_context = sc
        self._sql_context = SQLContext(sparkContext=sc)
        self._session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
            
    def get_sql_context(self):
        return self._sql_context

    def get_spark_session(self):
        return self._session

    def get_spark_context(self):
        return self._spark_context
    
    def get_num_cpus(self):
        return int(self.conf['spark.executor.instances']) * int(self.conf['spark.executor.cores'])

    def end(self):
        if self.use_bigdl:
            stop_orca_context()
            #pass
        else:
            self._spark_context.stop()
