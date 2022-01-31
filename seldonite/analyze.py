from typing import List

import pyspark.sql as psql

from seldonite import collect

from geotext import GeoText
from geopy import geocoders

class Analyze():
    collector: collect.Collector

    def __init__(self, collector):
        self.collector = collector

    def keywords_over_time(self, keywords: List[str]):
        self.collector._check_args()

        spark_builder = self.collector._get_spark_builder()
        with spark_builder.start_session() as spark_manager:
            df = self.collector._fetch(spark_manager)
            
            df = df.withColumn('all_text', psql.functions.concat(df['title'], psql.functions.lit(' '), df['text']))
            for keyword in keywords:
                df.withColumn(keyword, df['all_text'].like(f"%{keyword}%"))

            #df.groupby

            return df


        def proportion_of_countries(self):
            self.collector._check_args()

            spark_builder = self.collector._get_spark_builder()

            with spark_builder.start_session() as spark_manager:
                df = self.collector._fetch(spark_manager)

                df = df.withColumn('all_text', psql.functions.concat(df['headline'], psql.functions.lit(' '), df['short_description']))

                locations = []   

                for i in df.rdd.collect():
                    places = GeoText(i.all_text)
                    if not places.cities:
                            locations.append(['missing'])
                    else:
                        locations.append(places.cities)

                with_country = []

                for i in locations:
                    if i == ['missing']:
                        with_country.append("missing")
                    else:
                        locator = geopy.geocoders.Nominatim(user_agent="MyCoder")
                        location = locator.geocode(i)
                        country = str(location).rsplit(', ', 1)[1]
                        with_country.append(country)

            return dict((i, with_country.count(i)) for i in with_country)