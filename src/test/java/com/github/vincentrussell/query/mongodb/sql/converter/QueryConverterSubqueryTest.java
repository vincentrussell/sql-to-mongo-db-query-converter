package com.github.vincentrussell.query.mongodb.sql.converter;


import org.bson.Document;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class QueryConverterSubqueryTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        System.getProperties().remove(QueryConverter.D_AGGREGATION_ALLOW_DISK_USE);
        System.getProperties().remove(QueryConverter.D_AGGREGATION_BATCH_SIZE);
    }
    
    @Test
    public void writeSimpleSubquery() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select * from(select borough, cuisine from Restaurants limit 1)");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$limit\": 1\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"borough\": 1,\n" + 
        		"    \"cuisine\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    
    @Test
    public void writeSimpleSubqueryAlias_ProjectLimit() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.borough from(select borough, cuisine from Restautants limit 2) as c limit 1");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restautants.aggregate([{\n" + 
        		"  \"$limit\": 2\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"borough\": 1,\n" + 
        		"    \"cuisine\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 1\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"borough\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSimpleSubqueryAliasGroup_WhereProject() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.cuisine, c.c as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine limit 6000) as c where c.cuisine = 'Hamburgers' and c.borough ='Manhattan'");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"borough\": \"$borough\",\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 6000\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"borough\": \"$_id.borough\",\n" + 
        		"    \"cuisine\": \"$_id.cuisine\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$and\": [\n" + 
        		"      {\n" + 
        		"        \"cuisine\": \"Hamburgers\"\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"borough\": \"Manhattan\"\n" + 
        		"      }\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"cuisine\": 1,\n" + 
        		"    \"c\": \"$c\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSimpleSubqueryAliasGroup_WhereProjectGroup() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine limit 3000) as c where c.cuisine = 'Italian' group by cuisine");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"borough\": \"$borough\",\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 3000\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"borough\": \"$_id.borough\",\n" + 
        		"    \"cuisine\": \"$_id.cuisine\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"cuisine\": \"Italian\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": \"$c\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSimpleSubqueryAliasGroupSort_WhereProjectGroup() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine order by count(*) asc limit 30) as c where c.c > 100 group by cuisine");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"borough\": \"$borough\",\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"c\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 30\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"borough\": \"$_id.borough\",\n" + 
        		"    \"cuisine\": \"$_id.cuisine\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"c\": {\n" + 
        		"      \"$gt\": 100\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": \"$c\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSimpleSubqueryAliasGroup_WhereProjectGroupSort() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine limit 3000) as c where c.c > 100 group by c.cuisine order by cuisine desc ");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"borough\": \"$borough\",\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 3000\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"borough\": \"$_id.borough\",\n" + 
        		"    \"cuisine\": \"$_id.cuisine\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"c\": {\n" + 
        		"      \"$gt\": 100\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": \"$c\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"_id\": -1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSimpleSubqueryAliasGroupSort_WhereProjectGroupSort() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine order by count(*) desc limit 3) as c where c.c > 1000 group by cuisine order by cuisine asc");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"borough\": \"$borough\",\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"c\": -1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 3\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"borough\": \"$_id.borough\",\n" + 
        		"    \"cuisine\": \"$_id.cuisine\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"c\": {\n" + 
        		"      \"$gt\": 1000\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": \"$c\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"_id\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeSubqueryJoinByOneGetMaxOfGroup() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select r.cuisine as cuisine, trest.totalrestaurats as total from Restaurants as r inner join (select cuisine, count(*) as totalrestaurats from Restaurants group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats asc, cuisine asc limit 15");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$group\": {\n" + 
        		"          \"_id\": \"$cuisine\",\n" + 
        		"          \"totalrestaurats\": {\n" + 
        		"            \"$sum\": 1\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"cuisine\": \"$_id\",\n" + 
        		"          \"totalrestaurats\": 1,\n" + 
        		"          \"_id\": 0\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$cuisine\",\n" + 
        		"              \"$cuisine\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"trest\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$trest\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"trest.totalrestaurats\": 1,\n" + 
        		"    \"cuisine\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 15\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"cuisine\": \"$cuisine\",\n" + 
        		"    \"total\": \"$trest.totalrestaurats\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeSubqueryJoinByTwoGetMaxOfGroup() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select r.cuisine as cuisine, r.borough as borough, brest.totalrestaurats as total from Restaurants as r inner join (select cuisine, borough, count(*) as totalrestaurats from Restaurants group by cuisine, borough) as brest on r.cuisine = brest.cuisine and r.borough = brest.borough order by r.cuisine asc, r.borough asc limit 15");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\",\n" + 
        		"      \"borough\": \"$borough\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$group\": {\n" + 
        		"          \"_id\": {\n" + 
        		"            \"cuisine\": \"$cuisine\",\n" + 
        		"            \"borough\": \"$borough\"\n" + 
        		"          },\n" + 
        		"          \"totalrestaurats\": {\n" + 
        		"            \"$sum\": 1\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"cuisine\": \"$_id.cuisine\",\n" + 
        		"          \"borough\": \"$_id.borough\",\n" + 
        		"          \"totalrestaurats\": 1,\n" + 
        		"          \"_id\": 0\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$cuisine\",\n" + 
        		"                  \"$cuisine\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$borough\",\n" + 
        		"                  \"$borough\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"brest\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$brest\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"cuisine\": 1,\n" + 
        		"    \"borough\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 15\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"cuisine\": \"$cuisine\",\n" + 
        		"    \"borough\": \"$borough\",\n" + 
        		"    \"total\": \"$brest.totalrestaurats\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeTwoSubqueriesJoinByOneAndTwoGetMaxOfTwoGroups() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select r.cuisine as cuisine, r.borough as borough, trest.totalrestaurats as total, brest.totalrestaurats as local from Restaurants as r inner join (select cuisine, count(*) as totalrestaurats from Restaurants group by cuisine) as trest on r.cuisine = trest.cuisine inner join (select cuisine, borough, count(*) as totalrestaurats from Restaurants group by cuisine, borough) as brest on r.cuisine = brest.cuisine and r.borough = brest.borough order by cuisine asc, r.borough asc limit 15");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$group\": {\n" + 
        		"          \"_id\": \"$cuisine\",\n" + 
        		"          \"totalrestaurats\": {\n" + 
        		"            \"$sum\": 1\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"cuisine\": \"$_id\",\n" + 
        		"          \"totalrestaurats\": 1,\n" + 
        		"          \"_id\": 0\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$cuisine\",\n" + 
        		"              \"$cuisine\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"trest\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$trest\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\",\n" + 
        		"      \"borough\": \"$borough\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$group\": {\n" + 
        		"          \"_id\": {\n" + 
        		"            \"cuisine\": \"$cuisine\",\n" + 
        		"            \"borough\": \"$borough\"\n" + 
        		"          },\n" + 
        		"          \"totalrestaurats\": {\n" + 
        		"            \"$sum\": 1\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"cuisine\": \"$_id.cuisine\",\n" + 
        		"          \"borough\": \"$_id.borough\",\n" + 
        		"          \"totalrestaurats\": 1,\n" + 
        		"          \"_id\": 0\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$cuisine\",\n" + 
        		"                  \"$cuisine\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$borough\",\n" + 
        		"                  \"$borough\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"brest\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$brest\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"cuisine\": 1,\n" + 
        		"    \"borough\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 15\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"cuisine\": \"$cuisine\",\n" + 
        		"    \"borough\": \"$borough\",\n" + 
        		"    \"total\": \"$trest.totalrestaurats\",\n" + 
        		"    \"local\": \"$brest.totalrestaurats\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeJoinInSubqueryByOne() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select t.cuisine, max(t.total) as maxi from (select r.cuisine as cuisine, trest.totalrestaurats as total from Restaurants as r inner join (select cuisine, count(*) as totalrestaurats from Restaurants group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats desc, cuisine asc limit 15) as t group by t.cuisine");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$group\": {\n" + 
        		"          \"_id\": \"$cuisine\",\n" + 
        		"          \"totalrestaurats\": {\n" + 
        		"            \"$sum\": 1\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"cuisine\": \"$_id\",\n" + 
        		"          \"totalrestaurats\": 1,\n" + 
        		"          \"_id\": 0\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$cuisine\",\n" + 
        		"              \"$cuisine\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"trest\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$trest\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"trest.totalrestaurats\": -1,\n" + 
        		"    \"cuisine\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 15\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"cuisine\": \"$cuisine\",\n" + 
        		"    \"total\": \"$trest.totalrestaurats\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$cuisine\",\n" + 
        		"    \"maxi\": {\n" + 
        		"      \"$max\": \"$total\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"maxi\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeJoinInSubqueryAndJoinAgain() throws ParseException, IOException {
    	QueryConverter queryConverter = new QueryConverter("select t.cuisine as cuisine, max(t.total) as maxi, count(*) as coxi from Restaurants as r inner join (select r.cuisine as cuisine, r.borough as borough, trest.totalrestaurats as total from Restaurants as r inner join (select cuisine, count(*) as totalrestaurats from Restaurants group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats desc, cuisine asc, borough limit 15) as t on r.cuisine = t.cuisine and r.borough = t.borough group by t.cuisine");
    	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"Restaurants\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"cuisine\": \"$cuisine\",\n" + 
        		"      \"borough\": \"$borough\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$lookup\": {\n" + 
        		"          \"from\": \"Restaurants\",\n" + 
        		"          \"let\": {\n" + 
        		"            \"cuisine\": \"$cuisine\"\n" + 
        		"          },\n" + 
        		"          \"pipeline\": [\n" + 
        		"            {\n" + 
        		"              \"$group\": {\n" + 
        		"                \"_id\": \"$cuisine\",\n" + 
        		"                \"totalrestaurats\": {\n" + 
        		"                  \"$sum\": 1\n" + 
        		"                }\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$project\": {\n" + 
        		"                \"cuisine\": \"$_id\",\n" + 
        		"                \"totalrestaurats\": 1,\n" + 
        		"                \"_id\": 0\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$match\": {\n" + 
        		"                \"$expr\": {\n" + 
        		"                  \"$eq\": [\n" + 
        		"                    \"$$cuisine\",\n" + 
        		"                    \"$cuisine\"\n" + 
        		"                  ]\n" + 
        		"                }\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ],\n" + 
        		"          \"as\": \"trest\"\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$unwind\": {\n" + 
        		"          \"path\": \"$trest\",\n" + 
        		"          \"preserveNullAndEmptyArrays\": false\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$sort\": {\n" + 
        		"          \"trest.totalrestaurats\": -1,\n" + 
        		"          \"cuisine\": 1,\n" + 
        		"          \"borough\": 1\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$limit\": 15\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$project\": {\n" + 
        		"          \"_id\": 0,\n" + 
        		"          \"cuisine\": \"$cuisine\",\n" + 
        		"          \"borough\": \"$borough\",\n" + 
        		"          \"total\": \"$trest.totalrestaurats\"\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$cuisine\",\n" + 
        		"                  \"$cuisine\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$borough\",\n" + 
        		"                  \"$borough\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$t.cuisine\",\n" + 
        		"    \"maxi\": {\n" + 
        		"      \"$max\": \"$t.total\"\n" + 
        		"    },\n" + 
        		"    \"coxi\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"cuisine\": \"$_id\",\n" + 
        		"    \"maxi\": 1,\n" + 
        		"    \"coxi\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    private static Document document(String key, Object... values) {
        Document document = new Document();
        if (values !=null && values.length > 1) {
            document.put(key, Arrays.asList(values));
        } else if (values!=null) {
            document.put(key, values[0]);
        } else {
            document.put(key, values);
        }
        return document;
    }

    private static Document documentValuesArray(String key, Object... values) {
        return new Document(key,Arrays.asList(values));
    }

}
