from contextlib import contextmanager
import os

from bigdl.orca import init_orca_context, stop_orca_context
from pyspark import SparkContext, SparkConf
import pyspark.sql as psql


class SparkBuilder():
    def __init__(self, master, name='seldonite_app', archives=[], executor_cores=16, executor_memory='128g', driver_cores=2, driver_memory='128g', num_executors=1, python_executable='python', keep_alive=False, spark_conf={}):

        self.use_bigdl_flag = False
        self.keep_alive = keep_alive
        # address of spark master node
        self.spark_master_url = master
        self.archives = archives
        self.packages = []
        self.conf = {}
        # add user set spark conf
        self.conf.update(spark_conf.items())

        self.conf['spark.app.name'] = name

        # set driver specs
        self.conf['spark.driver.cores'] = driver_cores
        self.conf['spark.driver.memory'] = driver_memory

        # specify pod size
        self.conf['spark.executor.cores'] = str(executor_cores)
        self.conf['spark.executor.memory'] = executor_memory

        # increase some timeouts
        self.conf['spark.sql.broadcastTimeout'] = '1800'

        # trying to prevent large task timeouts
        self.conf['spark.sql.autoBroadcastJoinThreshold'] = '-1'

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
            self.conf['spark.ui.showConsoleProgress'] = 'true'

            os.environ['PYSPARK_PYTHON'] = python_executable

    def use_bigdl(self):
        self.use_bigdl_flag = True

    def set_conf(self, key, value):
        self.conf[key] = value

    def add_package(self, package):
        self.packages.append(package)

    def add_archive(self, archive):
        self.archives.append(archive)

    def use_spark_nlp(self):
        self.add_package('com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.1')
        self.set_conf('spark.kryoserializer.buffer.max', '2000M')
        self.set_conf("spark.driver.maxResultSize", "0")

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
            if not self.keep_alive:
                spark_manager.stop()

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
                    master=f"local[{conf['spark.driver.cores']}]",
                    conf=spark_conf
                )

        self._spark_context = sc
        self._sql_context = psql.SQLContext(sparkContext=sc)
        self._session = psql.SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
            
    def get_sql_context(self):
        return self._sql_context

    def get_spark_session(self):
        return self._session

    def get_spark_context(self):
        return self._spark_context
    
    def get_num_cpus(self):
        return int(self.conf['spark.executor.instances']) * int(self.conf['spark.executor.cores'])

    def stop(self):
        if self.use_bigdl:
            stop_orca_context()
            #pass
        else:
            self._spark_context.stop()

def batch(df, max_rows=None):
    assert max is not None

    num_partitions = max(1, int(df.count() / max_rows))

    df = df.withColumn('_row_id', psql.functions.monotonically_increasing_id())
    # Using ntile() because monotonically_increasing_id is discontinuous across partitions
    df = df.withColumn('_partition', psql.functions.ntile(num_partitions).over(psql.window.Window.orderBy(df._row_id))) 
    df.cache()

    for i in range(num_partitions):
        df_batch = df.filter(df._partition == i+1).drop('_row_id', '_partition')

        yield df_batch
