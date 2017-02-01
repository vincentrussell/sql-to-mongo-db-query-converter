# sql-to-mongo-db-query-converter

sql-to-mongo-db-query-converter helps you build quieres for MongoDb based on Queries provided in SQL.   

## Maven

Add a dependency to `com.github.vincentrussell:sql-to-mongo-db-query-converter`. 

```
<dependency>
   <groupId>com.github.vincentrussell</groupId>
   <artifactId>sql-to-mongo-db-query-converter</artifactId>
   <version>1.0</version>
</dependency>
```

## Requirements
- JDK 1.7 or higher

## Running it from Java

```
QueryConverter queryConverter = new QueryConverter("select column1 from my_table where value NOT IN ("theValue1","theValue2","theValue3")");
MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
String collection = mongoDBQueryHolder.getCollection();
Document query = mongoDBQueryHolder.getQuery();
Document projection = mongoDBQueryHolder.getProjection();
Document sort = mongoDBQueryHolder.getSort();
```

## Running it as a standalone jar

```
java -jar sql-to-mongo-db-query-converter-1.0-standalone.jar -s sql.file -d destination.json
```
### Options

```
usage: com.github.vincentrussell.query.mongodb.sql.converter.Main [-s
       <arg> | -sql <arg> | -i]   [-d <arg> | -h <arg>]  [-db <arg>] [-a
       <arg>] [-u <arg>] [-p <arg>] [-b <arg>]
 -s,--sourceFile <arg>        the source file.
 -sql,--sql <arg>             the sql select statement
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
java -jar target/sql-to-mongo-db-query-converter-1.0-standalone.jar -i
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
select * from my_table where regexMatch(column,'^[ae"gaf]+$') = true


******Result:*********

db.my_table.find({
  "column": {
    "$regex": "^[ae\"gaf]+$"
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
}])
```

###Direct Mongo Integration

You can run the queries against an actual mongodb database and take a look at the results.  The default return batch size is 50.

```
java -jar target/sql-to-mongo-db-query-converter-1.0-SNAPSHOT-standalone.jar -i -h localhost:3086 -db local -b 5
Enter input sql:


select borough, cuisine, count(*) from my_collection GROUP BY borough, cuisine ORDER BY count(*) DESC;


******Query Results:*********

[{
	"_id" : {
		"borough" : "Manhattan",
		"cuisine" : "American "
	},
	"count" : 3205
},{
	"_id" : {
		"borough" : "Brooklyn",
		"cuisine" : "American "
	},
	"count" : 1273
},{
	"_id" : {
		"borough" : "Queens",
		"cuisine" : "American "
	},
	"count" : 1040
},{
	"_id" : {
		"borough" : "Brooklyn",
		"cuisine" : "Chinese"
	},
	"count" : 763
},{
	"_id" : {
		"borough" : "Queens",
		"cuisine" : "Chinese"
	},
	"count" : 728
}]

more results? (y/n): y
[{
	"_id" : {
		"borough" : "Manhattan",
		"cuisine" : "Café/Coffee/Tea"
	},
	"count" : 680
},{
	"_id" : {
		"borough" : "Manhattan",
		"cuisine" : "Italian"
	},
	"count" : 621
},{
	"_id" : {
		"borough" : "Manhattan",
		"cuisine" : "Chinese"
	},
	"count" : 510
},{
	"_id" : {
		"borough" : "Manhattan",
		"cuisine" : "Japanese"
	},
	"count" : 438
},{
	"_id" : {
		"borough" : "Bronx",
		"cuisine" : "American "
	},
	"count" : 411
}]

more results? (y/n): n
```

# Change Log

## [1.3](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.3) (2017-01-31)

**Enhancements:**

- Added the ability to provide field types for columns passed into the query via Java API.  See QueryConverterTest for examples.

## [1.2](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.2) (2016-11-30)

**Bugs:**

- Fix bug with IN and NOT IN expressions from not converting properly to mongo format properly


## [1.1](https://github.com/vincentrussell/sql-to-mongo-db-query-converter/tree/sql-to-mongo-db-query-converter-1.1) (2016-10-05)

**Bugs:**

- Fix bug with not being able to parse like queries
