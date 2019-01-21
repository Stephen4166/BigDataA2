# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/a2

# COMMAND ----------

# q1
def mapper(line):
  list = line.split("\t")
  current = list[0]
  current = int(current)
  temp = list[1].split(",")
  for i in range(0, len(temp)):
    temp[i] = int(temp[i])
  pairs = []
  for item in temp:
    if current < item:
      pairs.append((current, item))
    else:
      pairs.append((item, current))
  result = []
  common = set(temp)
  for item in pairs:
    result.append((item, common))
  return result

def reducer(set1, set2):
  return set1.intersection(set2)

sourceFile = sc.textFile("/FileStore/tables/a2/soc_LiveJournal1Adj-2d179.txt").filter(lambda line: line.split("\t")[1] != '')
output1 = sourceFile.flatMap(mapper).reduceByKey(reducer).sortByKey().filter(lambda x: bool(x[1]))
output1.collect()

# COMMAND ----------

# q2
temp = output1.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: x[1], False)
top10 = sc.parallelize(temp.take(10))
top10.collect()

# COMMAND ----------

info = sc.textFile("dbfs:/FileStore/tables/a2/userdata.txt").map(lambda line: (int(line.split(",")[0]), line.split(",")[1:]))
infoAKey = top10.map(lambda x: (x[0][0],[x[0][1], x[1]]))
infoA = infoAKey.join(info).map(lambda x: (x[1][0][0], [x[0], x[1][0][1]] + x[1][1]))
infoAB = infoA.join(info).sortByKey()
output2 = infoAB.map(lambda x: (x[1][0][1], [x[1][0][0]]+x[1][0][2:5], [x[0]]+x[1][1][:3])).sortBy(lambda x: x[0])
for item in output2.collect():
  print(str(item[0]) + "\t"+item[1][1]+"\t"+item[1][2].ljust(10)+"\t"+item[1][3] + "\t"+item[2][1]+"\t"+item[2][2].ljust(10)+"\t"+item[2][3])

# COMMAND ----------

# q3
# Read all the files
movies = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/FileStore/tables/a2/movies.csv').rdd
ratings = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/FileStore/tables/a2/ratings.csv').rdd
tags = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/FileStore/tables/a2/tags.csv').rdd

# 3-1a
ratingsKV = ratings.map(lambda x: (x['movieId'], (x['rating'], 1)))
ratingsAVG = ratingsKV.reduceByKey(lambda a,b: ((a[0]+b[0]), (a[1]+b[1]))).mapValues(lambda x: x[0]/x[1]).sortBy(lambda x: x[1])
ratingsAVG.collect()

# COMMAND ----------

# 3-1b
moviesTitle = movies.map(lambda x: (x[0], x[1]))
lowest10 = sc.parallelize(ratingsAVG.take(10))
output3_1b = moviesTitle.join(lowest10).map(lambda x: (x[1][0], x[1][1]))
for item in output3_1b.collect():
  print(item[0].ljust(50) + "\t" + str(item[1]))

# COMMAND ----------

# 3-2
actionMovies = tags.filter(lambda x: x['tag'] == "action").map(lambda x: (x[1], x[2]))
output3_2 = actionMovies.join(ratingsAVG).mapValues(lambda x: x[1]).join(moviesTitle)
for k in output3_2.collect():
  print(str(k[0]).ljust(10) + "\t" + k[1][1].ljust(50) + "\t" + str(k[1][0]))

# COMMAND ----------

# 3-3
thrillers = movies.map(lambda x: (x[0], x[2].split("|"))).filter(lambda x: "Thriller" in x[1]).map(lambda x: (x[0], "T"))
output3_3 = thrillers.join(output3_2).map(lambda x: (x[0], x[1][1][1], x[1][1][0]))
for k in output3_3.collect():
  print(str(k[0]).ljust(10) + "\t" + k[1].ljust(50) + "\t" + str(k[2]))
