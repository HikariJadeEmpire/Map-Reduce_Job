# Map-Reduce_Job
> GOAL : Calculating the average personal incomes using a Map-Reduce job.


**Tools :**

- [Hadoop](https://github.com/HikariJadeEmpire/Map-Reduce_Job#hadoop)
- [Pyspark](https://github.com/HikariJadeEmpire/Map-Reduce_Job#pyspark)

***NOTE :*** These methods are executed on [Virtualbox](https://www.virtualbox.org/)

#
**Original file :** *Assignment.txt* ( without columns )
<br>

| person id | district id | personal income |
|-----------|-------------|-----------------|
| 10021 | 2 | 120000 |
| 10023 | 3 | 200000 |
| 10024 | 2 | 320000 |
| 10025 | 1 | 500000 |
| 10026 | 1 | 480000 |
| 10027 | 4 | 350000 |
| 10028 | 3 | 120000 |
| 10029 | 3 | 140000 |

<br>

# Hadoop
To initiate a Map-Reduce job in Hadoop, we need to create the following files: "mapper.py", "combiner.py", and "reducer.py" as outlined below :

- ***"mapper.py"***

```ruby

#!/usr/bin/env python

from operator import itemgetter
import sys

n = None
for line in sys.stdin:
  for i,s in enumerate(line.split(',')) :
    if i == 1 : n = s
    elif i == 2 :
      op = n + "\t{income}" .format(income = s)
      print(op)

```

<br>

- ***"combiner.py"***

```ruby

#!/usr/bin/env python

from operator import itemgetter
import sys

cur_district = ''
district_count = 0
sum_income = 0

for line in sys.stdin:
  line = line.split('\t')
  district, income = line[0], float( line[1] )

  if ( district != cur_district ) :
    if ( cur_district != '' ) :
      print( "%s\t%s\t%s"%( cur_district, sum_income, district_count ) )
    cur_district = district
    district_count = 1
    sum_income = income
  else :
    district_count += 1
    sum_income += income

print( "%s\t%s\t%s"%( cur_district, sum_income, district_count ) )

```

<br>

- ***"reducer.py"***

```ruby

#!/usr/bin/env python

from operator import itemgetter
import sys

cur_district = ''
district_count = 0
sum_income = 0

for line in sys.stdin:
  line = line.split('\t')
  district, n_sum_income, n_district_count = line[0], float( line[1] ), float( line[2] )

  if district != cur_district :
    if cur_district != '' :
      print( "%s\t%s.2f"%( cur_district, sum_income/district_count ) )
    cur_district = district
    district_count = n_district_count
    sum_income = n_sum_income
  else :
    district_count += n_district_count
    sum_income += n_sum_income

print( "%s\t%s.2f"%( cur_district, sum_income/district_count ) )

```
<br>

Next, we enter the **bash** command as follows: <br>

```ruby

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/cloudera/Assignment.txt \
-output /user/cloudera/as01 \
-mapper “python mapper2.py” \
-reducer “python reducer2.py” \
-combiner “python combiner.py” \
-file mapper2.py -file reducer2.py -file combiner.py

```
***NOTE :*** These methods are executed on [Virtualbox](https://www.virtualbox.org/)
<br>

<h3> Hadoop RESULT </h3>

XXX

<br>

# Pyspark
To commence a Map-Reduce job in PySpark, we need to define the required functions as follows:

<br>

```ruby

from operator import add

def map1( f ) :
  id, dis, inc = f.split( ',' )
  return str(dis) , ( int(inc) , 1)

def avg( val1, val2 ) :
  return ( ( val1[0] + val2[0] , val1[1] + val2[1] ) ) 

```
<br>

Then, we initiate the process.

```ruby

text = sc.textFile( ' /user/cloudera/Assignment.txt ' )

words = text.map( map1 ).sortByKey()

words_1 = words.reduceByKey( avg ).mapValues( lambda x : x[0] / x[1] )

```
<br>

<h3> Pyspark RESULT </h3>

XXX

<br>

# 
Go to Top : [:arrow_double_up: TOP]()
