package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class QueryConverterJoinTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        System.getProperties().remove(QueryConverter.D_AGGREGATION_ALLOW_DISK_USE);
        System.getProperties().remove(QueryConverter.D_AGGREGATION_BATCH_SIZE);
    }
    
    @Test
    public void writeInnerJoinByOneField() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.column1, t2.column2 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$column\",\n" + 
        		"              \"$column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"t2.column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByTwoFields() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.column1, t2.column2 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column and t2.column2 = t1.column2");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\",\n" + 
        		"      \"column2\": \"$column2\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$column2\",\n" + 
        		"                  \"$$column2\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"t2.column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByTwoNestedFields() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.nested1.column1, t2.nested2.column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.column = t2.nested2.column and t2.nested2.column2 = t1.nested1.column1");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.column\",\n" + 
        		"      \"nested1_column1\": \"$nested1.column1\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$nested1_column\",\n" + 
        		"                  \"$nested2.column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$nested2.column2\",\n" + 
        		"                  \"$$nested1_column1\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"nested1.column1\": 1,\n" + 
        		"    \"t2.nested2.column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    //mongovars are start with a lowercase letter
    @Test
    public void writeInnerJoinByOneFieldUpperCaseField() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.Column = t2.Column");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$column\",\n" + 
        		"              \"$Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneFieldWhereInBaseTable() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.Column = t2.Column where t1.whereColumn = \"whereValue\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"whereColumn\": \"whereValue\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$column\",\n" + 
        		"              \"$Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneFieldWhereInJoinTable() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.Column = t2.Column where t2.whereColumn = \"whereValue\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"whereColumn\": \"whereValue\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneFieldWhereInBothTables() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.Column = t2.Column where t1.whereColumn1 = \"whereValue1\" and t2.whereColumn2 = \"whereValue2\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"whereColumn1\": \"whereValue1\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"whereColumn2\": \"whereValue2\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneFieldWhereNestedInBothTables() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.Column = t2.Column where t1.nested1.whereColumn1 = \"whereValue1\" and t2.nested2.whereColumn2 = \"whereValue2\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneNestedFieldWhereNestedInBothTables() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column where t1.nested1.whereColumn1 = \"whereValue1\" and t2.nested2.whereColumn2 = \"whereValue2\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$nested1_column\",\n" + 
        		"                  \"$nested2.Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeInnerJoinByOneNestedFieldWhereNestedInBothTablesWithOr() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column where t1.nested1.whereColumn1 = \"whereValue1\" or t2.nested2.whereColumn2 = \"whereValue2\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$nested1_column\",\n" + 
        		"              \"$nested2.Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$or\": [\n" + 
        		"      {\n" + 
        		"        \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"t2.nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"      }\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeTwoInnerJoinByOneField() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.column1, t2.column2, t3.column3 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column inner join my_table3 as t3 on t1.column = t3.column");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$column\",\n" + 
        		"              \"$column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table3\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$column\",\n" + 
        		"              \"$column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t3\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t3\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"t2.column2\": 1,\n" + 
        		"    \"t3.column3\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeTwoInnerJoinByTwoFields() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.column1, t2.column2, t3.column3 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column and t2.column2 = t1.column2 inner join my_table3 as t3 on t1.column = t3.column and t2.column2 = t3.column2");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\",\n" + 
        		"      \"column2\": \"$column2\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$column2\",\n" + 
        		"                  \"$$column2\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table3\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"column\": \"$column\",\n" + 
        		"      \"t2_column2\": \"$t2.column2\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$column\",\n" + 
        		"                  \"$column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$t2_column2\",\n" + 
        		"                  \"$column2\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t3\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t3\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"t2.column2\": 1,\n" + 
        		"    \"t3.column3\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeTwoInnerJoinByOneNestedFieldWhereNestedInBothTables() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column inner join my_table3 as t3 on t1.nested1.Column = t3.nested3.Column where t1.nested1.whereColumn1 = \"whereValue1\" and t2.nested2.whereColumn2 = \"whereValue2\" and t3.nested3.whereColumn3 = \"whereValue3\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$nested1_column\",\n" + 
        		"                  \"$nested2.Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table3\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$and\": [\n" + 
        		"            {\n" + 
        		"              \"$expr\": {\n" + 
        		"                \"$eq\": [\n" + 
        		"                  \"$$nested1_column\",\n" + 
        		"                  \"$nested3.Column\"\n" + 
        		"                ]\n" + 
        		"              }\n" + 
        		"            },\n" + 
        		"            {\n" + 
        		"              \"nested3.whereColumn3\": \"whereValue3\"\n" + 
        		"            }\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t3\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t3\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeTwoInnerJoinByOneNestedFieldWhereNestedInBothTablesWithOr() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 inner join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column inner join my_table3 as t3 on t1.nested1.Column = t3.nested3.Column where (t1.nested1.whereColumn1 = \"whereValue1\" and t2.nested2.whereColumn2 = \"whereValue2\") or t3.nested3.whereColumn3 = \"whereValue3\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$nested1_column\",\n" + 
        		"              \"$nested2.Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table3\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$nested1_column\",\n" + 
        		"              \"$nested3.Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t3\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t3\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$or\": [\n" + 
        		"      {\n" + 
        		"        \"$and\": [\n" + 
        		"          {\n" + 
        		"            \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"          },\n" + 
        		"          {\n" + 
        		"            \"t2.nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"          }\n" + 
        		"        ]\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"t3.nested3.whereColumn3\": \"whereValue3\"\n" + 
        		"      }\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeTwoJoinByOneNestedFieldWhereNestedInBothTablesWithOr() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select t1.Column1, t2.Column2 from my_table as t1 join my_table2 as t2 on t1.nested1.Column = t2.nested2.Column join my_table3 as t3 on t1.nested1.Column = t3.nested3.Column where (t1.nested1.whereColumn1 = \"whereValue1\" and t2.nested2.whereColumn2 = \"whereValue2\") or t3.nested3.whereColumn3 = \"whereValue3\"");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {}\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table2\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$nested1_column\",\n" + 
        		"              \"$nested2.Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t2\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t2\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$lookup\": {\n" + 
        		"    \"from\": \"my_table3\",\n" + 
        		"    \"let\": {\n" + 
        		"      \"nested1_column\": \"$nested1.Column\"\n" + 
        		"    },\n" + 
        		"    \"pipeline\": [\n" + 
        		"      {\n" + 
        		"        \"$match\": {\n" + 
        		"          \"$expr\": {\n" + 
        		"            \"$eq\": [\n" + 
        		"              \"$$nested1_column\",\n" + 
        		"              \"$nested3.Column\"\n" + 
        		"            ]\n" + 
        		"          }\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ],\n" + 
        		"    \"as\": \"t3\"\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$unwind\": {\n" + 
        		"    \"path\": \"$t3\",\n" + 
        		"    \"preserveNullAndEmptyArrays\": false\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$or\": [\n" + 
        		"      {\n" + 
        		"        \"$and\": [\n" + 
        		"          {\n" + 
        		"            \"nested1.whereColumn1\": \"whereValue1\"\n" + 
        		"          },\n" + 
        		"          {\n" + 
        		"            \"t2.nested2.whereColumn2\": \"whereValue2\"\n" + 
        		"          }\n" + 
        		"        ]\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"t3.nested3.whereColumn3\": \"whereValue3\"\n" + 
        		"      }\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"Column1\": 1,\n" + 
        		"    \"t2.Column2\": 1\n" + 
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
