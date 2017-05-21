# Calculate the average stars for each business category
# Written by Dr. Yuhang Wang and Josh Gardner
'''
The goal is to compute the number of businesses, total review count, and number of 4-star or higher reviews for each neighborhood in each city. 
If a business has multiple neighborhoods, its review count and stars should be attributed to all of the neighborhoods. 
If the neighborhoods list is empty, then we will use 'Unknown' as the name of the neighborhood. 

To get results:
hadoop fs -getmerge part1_output part1_output.txt
'''
import json
from pyspark import SparkContext

sc = SparkContext(appName="PySparksi618w17avg_stars_per_category")

input_file = sc.textFile("hdfs:///var/si618w17/yelp_academic_dataset_business_updated.json")

def cat_star(data):
  res = []
  stars = data.get('stars', None)
  re_count = data.get('review_count', 0)

  if stars >=4:
    review = (re_count,1)
  else:
    review = (re_count,0)

  city = data.get('city',None)
  neighborhoods = data.get('neighborhoods',None)
  if neighborhoods == []:
    neighborhoods = ['Unknown']

  for i in neighborhoods:
    c_n = (city,i)
    res.append((c_n,review))
  return res


city_count = input_file.map(lambda line: json.loads(line)) \
                           .flatMap(cat_star) \
                           .mapValues(lambda x: (1, x)) \
                           .reduceByKey(lambda x, y: (x[0] + y[0], (x[1][0] + y[1][0],x[1][1]+y[1][1])))\
                           .map(lambda x: (x[0][0],x[0][1],x[1][0],x[1][1][0],x[1][1][1]))\
                           .sortBy(lambda x:(x[0],-x[2],x[1]))


city_count.map(lambda x:x[0]+'\t'+x[1]+'\t'+str(x[2])+'\t'+str(x[3])+'\t'+str(x[4])).saveAsTextFile("part1_output_1")

