conda env update -f ./seldonite/spark/pyspark_environment.yml
#conda run -n seldonite_spark pip install --no-deps -r ./no_deps_requirements.txt
conda pack -n seldonite_spark -f -o ./seldonite/spark/seldonite_spark_env.tar.gz