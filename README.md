# sql-to-mongo-db-query-converter [![Maven Central](https://img.shields.io/maven-central/v/com.github.vincentrussell/sql-to-mongo-db-query-converter.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.github.vincentrussell%22%20AND%20a:%22sql-to-mongo-db-query-converter%22) [![Build Status](https://travis-ci.org/vincentrussell/sql-to-mongo-db-query-converter.svg?branch=master)](https://travis-ci.org/vincentrussell/sql-to-mongo-db-query-converter)

sql-to-mongo-db-query-converter helps you build quieres for MongoDb based on Queries provided in SQL.   

## Maven

Add a dependency to `com.github.vincentrussell:sql-to-mongo-db-query-converter`. 

```
<dependency>
   <groupId>com.github.vincentrussell</groupId>
   <artifactId>sql-to-mongo-db-query-converter</artifactId>
   <version>1.14</version>
</dependency>
```

## Requirements
- JDK 1.7 or higher

## Running it from Java

```
QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 from my_table where value NOT IN ("theValue1","theValue2","theValue3")").build();
MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
String collection = mongoDBQueryHolder.getCollection();
Document query = mongoDBQueryHolder.getQuery();
Document projection = mongoDBQueryHolder.getProjection();
Document sort = mongoDBQueryHolder.getSort();
```

## Running it as a standalone jar

```
java -jar sql-to-mongo-db-query-converter-1.14-standalone.jar -s sql.file -d destination.json
```
### Options

```
usage: com.github.vincentrussell.query.mongodb.sql.converter.Main [-s
       <arg> | -sql <arg> | -i]   [-d <arg> | -h <arg>]  [-db <arg>] [-a
       <arg>] [-u <arg>] [-p <arg>] [-b <arg>]
 -s,--sourceFile <arg>        the source file.
 -sql,--sql <arg>             the select statement
 -i,--interactiveMode         interactive mode
 -d,--destinationFile <arg>   the destination file.  Defaults to
                              System.out
 -h,--host <arg>              hosts and ports in the following format
                              (host:port) default port is 27017
 -db,--database <arg>         mongo database
 -a,--auth database <arg>     auth mongo database
 -u,--username <arg>          usename
 -p,--password <arg>          password
 -b,--batchSize <arg>         batch size for query results
```

### Special Aggregation-specific System Properties

```
-DaggregationAllowDiskUse
Enables writing to temporary files. When set to true, aggregation operations can write data to the _tmp subdirectory in the dbPath directory.

-DaggregationBatchSize
To specify an initial batch size for the cursor
```

## Interactive mode

```
java -jar target/sql-to-mongo-db-query-converter-1.14-standalone.jar -i
Enter input sql:


select object.key1, object2.key3, object1.key4 from my_collection where object.key2 = 34 AND object2.key4 > 5


******Result:*********

db.my_collection.find({
  "$and": [
    {
      "key2": {
        "$numberLong": "34"
      }
    },
    {
      "object2.key4": {
        "$gt": {
          "$numberLong": "5"
        }
      }
    }
  ]
} , {
  "_id": 0,
  "object.key1": 1,
  "object2.key3": 1,
  "object1.key4": 1
})

```

##Available options

###Dates

```
select * from my_table where date(column,'YYY-MM-DD') >= '2016-12-12'


******Result:*********

db.my_table.find({
  "column": {
    "$gte": {
      "$date": 1452556800000
    }
  }
})
```

###Natural Language Dates

```
select * from my_table where date(column,'natural') >= '5000 days ago'


******Result:*********

db.my_table.find({
  "column": {
    "$gte": {
      "$date": 1041700019654
    }
  }
})
```

###Regex

```
select * from my_table where regexMatch(column,'^[ae"gaf]+$')


******Result:*********

db.my_table.find({
  "column": {
    "$regex": "^[ae\"gaf]+$"
  }
})
```

###NOT Regex match

```
select * from my_table where notRegexMatch(column,'^[ae"gaf]+$')


******Result:*********

db.my_table.find({
  "column": {
    "$not": /^[ae\"gaf]+$/
  }
})
```

###Distinct

```
select distinct column1 from my_table where value IS NULL


******Result:*********

db.my_table.distinct("column1" , {
  "value": {
    "$exists": false
  }
})
```

###Like

```
select * from my_table where value LIKE 'start%'


******Result:*********

db.my_table.find({
  "value": {
    "$regex": "^start.*$"
  }
})
```

###Like

```
select * from my_table where value NOT LIKE 'start%'


******Result:*********

db.my_table.find({
  "value": {
    "$not": /^start.*$/
  }
})
```

###In

```
select column1 from my_table where value IN ("theValue1","theValue2","theValue3")


******Result:*********

db.my_table.find({ 
	"value" : { 
		"$in" : ["theValue1","theValue2", "theValue3"] 
		}
})
```

###Not In

```
select column1 from my_table where value NOT IN ("theValue1","theValue2","theValue3")


******Result:*********

db.my_table.find({ 
	"value" : { 
		"$nin" : ["theValue1","theValue2", "theValue3"] 
		}
})
```

###Is True

```
select column1 from my_table where column = true


******Result:*********

db.my_table.find({ 
	"column" : true
})
```

###Is False

```
select column1 from my_table where column = false


******Result:*********

db.my_table.find({ 
	"column" : false
})
```

###Not True

```
select column1 from my_table where NOT column


******Result:*********

db.my_table.find({ 
	"value" : {$ne: true}
})
```


###ObjectId Support

```
select column1 from  where OBJECTID('_id') IN ('53102b43bf1044ed8b0ba36b', '54651022bffebc03098b4568')

******Result:*********

db.my_table.find({ 
	"_id" : {$in: [{$oid: "53102b43bf1044ed8b0ba36b"},{$oid: "54651022bffebc03098b4568"}]}
})
```

```
select column1 from  where OBJECTID('_id') = '53102b43bf1044ed8b0ba36b'

******Result:*********

db.my_table.find({ 
	"_id" : {$oid: "53102b43bf1044ed8b0ba36b"}
})
```

###Delete

```
delete from my_table where value IN ("theValue1","theValue2","theValue3")


******Result:*********

3 (number or records deleted)
```

###Group By (Aggregation)

```
select borough, cuisine, count(*) from my_collection WHERE borough LIKE 'Queens%' GROUP BY borough, cuisine ORDER BY count(*) DESC;


******Mongo Query:*********

db.my_collection.aggregate([{
  "$match": {
    "borough": {
      "$regex": "^Queens.*$"
    }
  }
},{
  "$group": {
    "_id": {
      "borough": "$borough",
      "cuisine": "$cuisine"
    },
    "count": {
      "$sum": 1
    }
  }
},{
  "$sort": {
    "count": -1
  }
},{
  "$project": {
    "borough": "$_id.borough",
    "cuisine": "$_id.cuisine",
    "count": 1,
    "_id": 0
  }
}])
```

###Having clause with aggregation

```
select Restaurant.cuisine, count(*) from Restaurants group by Restaurant.cuisine having count(*) > 3;


******Mongo Query:*********

db.Restaurants.aggregate([
                           {
                             "$group": {
                               "_id": "$Restaurant.cuisine",
                               "count": {
                                 "$sum": 1
                               }
                             }
                           },
                           {
                             "$match": {
                               "$expr": {
                                 "$gt": [
                                   "$count",
                                   3
                                 ]
                               }
                             }
                           },
                           {
                             "$project": {
                               "Restaurant.cuisine": "$_id",
                               "count": 1,
                               "_id": 0
                             }
                           }
                         ])
```

###Count without GROUP BY
```
select count(*) as c from table

******Mongo Query:*********
db.table.aggregate([{ "$group": { "_id": {}, "c": { "$sum": 1 } } },{ "$project": { "c": 1, "_id": 0 } }])
```

###Avg without GROUP BY

```
select avg(field) as avg from table

******Mongo Query:*********
db.table.aggregate([{ "$group": { "_id": {}, "avg": { "$avg": "$field" } } },{ "$project": { "avg": 1, "_id": 0 } }])
```

###Joins

```
select t1.column1, t2.column2 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column


******Result:*********

db.my_table.aggregate([
                   {
                     "$match": {}
                   },
                   {
                     "$lookup": {
                       "from": "my_table2",
                       "let": {
                         "column": "$column"
                       },
                       "pipeline": [
                         {
                           "$match": {
                             "$expr": {
                               "$eq": [
                                 "$$column",
                                 "$column"
                               ]
                             }
                           }
                         }
                       ],
                       "as": "t2"
                     }
                   },
                   {
                     "$unwind": {
                       "path": "$t2",
                       "preserveNullAndEmptyArrays": false
                     }
                   },
                   {
                     "$project": {
                       "_id": 0,
                       "column1": 1,
                       "t2.column2": 1
                     }
                   }
                 ])


or

select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column inner join my_table3 as t3 on t1.nested1.Column = t3.nested3.Column where t1.nested1.whereColumn1 = "whereValue1" and t2.nested2.whereColumn2 = "whereValue2" and t3.nested3.whereColumn3 = "whereValue3"


******Result:*********

db.my_table.aggregate([
                        {
                          "$match": {
                            "nested1.whereColumn1": "whereValue1"
                          }
                        },
                        {
                          "$lookup": {
                            "from": "my_table2",
                            "let": {
                              "nested1_column": "$nested1.Column"
                            },
                            "pipeline": [
                              {
                                "$match": {
                                  "$and": [
                                    {
                                      "$expr": {
                                        "$eq": [
                                          "$$nested1_column",
                                          "$nested2.Column"
                                        ]
                                      }
                                    },
                                    {
                                      "nested2.whereColumn2": "whereValue2"
                                    }
                                  ]
                                }
                              }
                            ],
                            "as": "t2"
                          }
                        },
                        {
                          "$unwind": {
                            "path": "$t2",
                            "preserveNullAndEmptyArrays": false
                          }
                        },
                        {
                          "$lookup": {
                            "from": "my_table3",
                            "let": {
                              "nested1_column": "$nested1.Column"
                            },
                            "pipeline": [
                              {
                                "$match": {
                                  "$and": [
                                    {
                                      "$expr": {
                                        "$eq": [
                                          "$$nested1_column",
                                          "$nested3.Column"
                                        ]
                                      }
                                    },
                                    {
                                      "nested3.whereColumn3": "whereValue3"
                                    }
                                  ]
                                }
                              }
                            ],
                            "as": "t3"
                          }
                        },
                        {
                          "$unwind": {
                            "path": "$t3",
                            "preserveNullAndEmptyArrays": false
                          }
                        },
                        {
                          "$project": {
                            "_id": 0,
                            "c1": "$Column1",
                            "c2": "$t2.Column2",
                            "c3": "$t3.Column3"
                          }
                        }
                      ])


```


###Alias

```
select object.key1 as key1, object2.key3 as key3, object1.key4 as key4 from my_collection where object.key2 = 34 AND object2.key4 > 5;


******Mongo Query:*********

db.Restaurants.aggregate([{
  "$match": {
    "$and": [
      {
        "Restaurant.cuisine": "American"
      },
      {
        "Restaurant.borough": {
          "$gt": "N"
        }
      }
    ]
  }
},{
  "$project": {
    "_id": 0,
    "key1": "$Restaurant.borough",
    "key3": "$Restaurant.cuisine",
    "key4": "$Restaurant.address.zipcode"
  }
}])
```

###Alias Group By (Aggregation)

```
select borough as b, cuisine as c, count(*) as co from my_collection WHERE borough LIKE 'Queens%' GROUP BY borough, cuisine ORDER BY count(*) DESC;


******Mongo Query:*********

db.my_collection.aggregate([{
  "$match": {
    "borough": {
      "$regex": "^Queens.*$"
    }
  }
},{
  "$group": {
    "_id": {
      "borough": "$borough",
      "cuisine": "$cuisine"
    },
    "co": {
      "$sum": 1
    }
  }
},{
  "$sort": {
    "co": -1
  }
},{
  "$project": {
    "b": "$_id.borough",
    "c": "$_id.cuisine",
    "co": 1,
    "_id": 0
  }
}])
```

###Offset

```
select * from table limit 3 offset 4
or
select a, count(*) from table group by a limit 3 offset 4


******Result:*********

is equivalent to the $skip function in mongodb json query language
```

###Using column names that start with a number.  Sorround it in quotes:

```
SELECT * FROM tb_test WHERE "3rd_column" = 10
```

###Direct Mongo Integration

You can run the queries against an actual mongodb database and take a look at the results.  The default return batch size is 50.

```
java -jar target/sql-to-mongo-db-query-converter-1.14-SNAPSHOT-standalone.jar -i -h localhost:3086 -db local -b 5
Enter input sql:


select borough, cuisine, count(*) from my_collection GROUP BY borough, cuisine ORDER BY count(*) DESC;


******Query Results:*********

[{
	"borough" : "Manhattan",
	"cuisine" : "American ",
	"count" : 3205
},{
	"borough" : "Brooklyn",
	"cuisine" : "American ",
	"count" : 1273
},{
	"borough" : "Queens",
	"cuisine" : "American ",
	"count" : 1040
},{
	"borough" : "Brooklyn",
	"cuisine" : "Chinese",
	"count" : 763
},{
	"borough" : "Queens",
	"cuisine" : "Chinese",
	"count" : 728
}]

more results? (y/n): y
[{
	"borough" : "Manhattan",
	"cuisine" : "Café/Coffee/Tea",
	"count" : 680
},{
	"borough" : "Manhattan",
	"cuisine" : "Italian",
	"count" : 621
},{
	"borough" : "Manhattan",
	"cuisine" : "Chinese",
	"count" : 510
},{
	"borough" : "Manhattan",
	"cuisine" : "Japanese",
	"count" : 438
},{
	"borough" : "Bronx",
	"cuisine" : "American ",
	"count" : 411
}]

more results? (y/n): n
```

# Change Log

## [1.15](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.15) (TBD)

**Enhancements:**

- Added getQueryAsDocument to the QueryConverter
- Upgraded guava to 24.1.1-jre
- Added "Loop mode" with -l when running in CLI mode.

**Bugs:**

- Added checkstyle to build process
- Code cleanup
- Negative numbers are supported




# Change Log

## [1.14](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.14) (2020-07-22)

**Enhancements:**

- Support for Double values
- Support for Timestamp values, i.e: {ts '2019-10-11 12:12:23.234'}
- Support for Date values, i.e: {d '2019-10-11'}


## [1.13](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.13) (2020-07-16)

**Enhancements:**

- Support for NOT LIKE queries
- added notRegexMatch function

**Bugs:**

- NOT expressions with parentheses not working properly, i.e: NOT (Country = 'Albania')

## [1.12](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.12) (2020-07-07)

**Enhancements:**

- Fix queries using IN keyword.  They were broken in 1.11

## [1.11](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.11) (2020-05-22)

**Enhancements:**

- Support for subqueries translated to aggregation steps in a recursive way.
- Created a builder for the QueryConverter class and depreciated the constructors for the QueryConverterClass
- Created the ability to prove the following options to aggregation: aggregationAllowDiskUse and aggregationBatchSize
- Support for having clause
- Upgrading com.github.jsqlparser:jsqlparser from v1.4 to v3.1
- Supporting group operations (avg, max, min, count, sum) without "group by" clause for performing a total group.

# Change Log

## [1.10](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.10) (2020-02-01)

**Enhancements:**

- Added the ability to use sql aliases that will do a mongo $project.
- Added the ability to use offset syntax in sql to skip records
- Added the ability to use lookup-let-pipeline strategy of mongo 3.6 and $expr for performing joins.
- Added the ablility to join multiple tables and use them in where or project clause. In the new test class are many examples.

## [1.9](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.9) (2019-04-02)

**Enhancements:**

- Added the capability for nested custom functions

## [1.8](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.8) (2019-02-01)

**Enhancements:**

- Upgraded jsqlparser to version 1.4
- Added support for deep nested queries; i.e: select * from my_table where a.b.c.d.e.key = value


## [1.7](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.7) (2018-11-13)

**Enhancements:**

- Equals, Not Equals, In and Not In ObjectId query support
- regexMatch function can be used with or without equals sign


## [1.6](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.6) (2018-07-24)

**Bugs:**

- remove double quotes from column names when used in IS NULL query

## [1.5](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.5) (2018-06-15)

**Enhancements:**

- upgrade jsqlparser to version 1.2
- create flatter structure when chaining ORs and ANDs together


## [1.4](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.4) (2018-03-03)

**Enhancements:**

- Added support NOT operator on parentheses
- Added support for delete SQL statements

## [1.3.4](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.3.4) (2018-01-27)

**Enhancements:**

- Added the ability to pass down custom sql functions down to mongo

## [1.3.2](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.3.2) (2017-07-02)

**Enhancements:**

- Added the ability to support queries on boolean fields
- UTF-8 support

## [1.3.1](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.3.1) (2017-02-19)

**Enhancements:**

- Added the ability to have default type like Number, String, or Date
- Added the ability to provide a type for each field like Number, String, Date

## [1.3](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.3) (2017-01-31)

**Enhancements:**

- Added the ability to provide field types for columns passed into the query via Java API.  See QueryConverterTest for examples.

## [1.2](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.2) (2016-11-30)

**Bugs:**

- Fix bug with IN and NOT IN expressions from not converting properly to mongo format properly


## [1.1](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.1) (2016-10-05)

**Bugs:**

- Fix bug with not being able to parse like queries
