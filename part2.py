'''
Find out the distribution of the number of distinct cities that Yelp users wrote reviews in. 
'''

import json
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

sc = SparkContext(appName="PySparksi618w17_hw5_part2")
SQLContext = SQLContext(sc)

input_business = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_business_updated.json")
input_reviews = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_review_updated.json")


def uniq_cities(x,y):
    if y[0] not in x:
      x.append(y[0])
    return x


city_list = input_business.map(lambda line: json.loads(line)) \
                          .map(lambda p: Row(business_id = p['business_id'], city = p['city']))

city_list_df = SQLContext.inferSchema(city_list)
city_list_df.registerTempTable("City")

review_list = input_reviews.map(lambda line: json.loads(line)) \
                          .map(lambda p: Row(business_id = p['business_id'], user_id = p['user_id'], stars = p['stars']))

review_list_df = SQLContext.inferSchema(review_list)
review_list_df.registerTempTable("Review")

total_df = SQLContext.sql("SELECT DISTINCT user_id, city FROM City JOIN Review ON City.business_id = Review.business_id")

total_list = total_df.rdd.map(tuple) \
                         .mapValues(lambda x: 1) \
                         .reduceByKey(lambda a,b:a+b) \
                         .map(lambda x:(x[1]))

hist = total_list.histogram(range(total_list.min()-1,total_list.max()+2))

final = sc.parallelize(zip(hist[0][1:-1], hist[1][1:]))

header = sc.parallelize(('cities','yelp users'))


header.union(final.map(lambda x: str(x[0])+','+str(x[1]))).saveAsTextFile("part2_output")
