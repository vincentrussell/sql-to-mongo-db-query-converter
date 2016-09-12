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
Document query = mongoDBQueryHolder.getQuery();
Document projection = mongoDBQueryHolder.getProjection();
```

## Running it as a standalone jar

```
java -jar sql-to-mongo-db-query-converter-1.0-standalone.jar -s sql.file -d destination.json
```
### Options

```
usage: com.github.vincentrussell.query.mongodb.sql.converter.Main [-s
       <arg>] [-d <arg>] [-i]
 -s,--sourceFile <arg>        the source file.
 -d,--destinationFile <arg>   the destination file.  Defaults to
                              System.out
 -i,--interactiveMode         interactive mode
```

## Interactive mode

```
java -jar target/sql-to-mongo-db-query-converter-1.0-SNAPSHOT-standalone.jar -i
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