package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryConverterTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void before() {
        System.getProperties().remove(QueryConverter.D_AGGREGATION_ALLOW_DISK_USE);
        System.getProperties().remove(QueryConverter.D_AGGREGATION_BATCH_SIZE);
    }

    @Test
    public void selectAllFromTableWithoutWhereClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(-1,mongoDBQueryHolder.getLimit());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    public void selectAllFromTableWithoutWhereClauseLimit() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table\n" +
                "limit 10");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(10,mongoDBQueryHolder.getLimit());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    public void selectAllFromTableWithoutWhereClauseOrderByField1() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table\n" +
                "order by field_1 ASC, field_2 DESC");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("field_1",1).append("field_2",-1),mongoDBQueryHolder.getSort());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLong() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value=1");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",1L),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void selectDistinctFieldFromTableWithSimpleWhereClauseLong() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select DISTINCT column1 from my_table where value=1");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(1, mongoDBQueryHolder.getProjection().size());
        assertEquals(document("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals("my_table", mongoDBQueryHolder.getCollection());
        assertEquals(document("value", 1L), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectDistinctMultipleFields() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("cannot run distinct one more than one column"));
        new QueryConverter("select DISTINCT column1, column2 from my_table where value=1");
    }

    @Test
    public void selectDistinctAll() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("cannot run distinct one more than one column"));
        new QueryConverter("select DISTINCT * from my_table where value=1");
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNotNull() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value IS NOT NULL");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$exists",true)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatch() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where regexMatch(column,'^[ae\"gaf]+$') = true ");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatchWithOptions() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where regexMatch(column,'^[ae\"gaf]+$','si') = true ");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$").append("$options","si")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void dateMatchGTE() throws ParseException {
        dateTest(">=","$gte");
    }

    @Test
    public void dateMatchGT() throws ParseException {
        dateTest(">","$gt");
    }

    @Test
    public void dateMatchLTE() throws ParseException {
        dateTest("<=","$lte");
    }

    @Test
    public void dateMatchLT() throws ParseException {
        dateTest("<","$lt");
    }

    private void dateTest(String equation, String mongoFunction) throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where date(column,'YYY-MM-DD') "+equation+" '2016-12-12' ");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document(mongoFunction,new Date(1452556800000L))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithLikeQuery() throws ParseException {
        likeTest("start%","^start.*$");
    }

    @Test
    public void selectAllFromTableWithLikeQueryMultipleWildcards() throws ParseException {
        likeTest("%start%","^.*start.*$");
    }

    @Test
    public void selectAllFromTableWithLikeQueryOneChar() throws ParseException {
        likeTest("start_","^start.{1}$");
    }

    @Test
    public void selectAllFromTableWithLikeQueryOneCharMultipleWildcards() throws ParseException {
        likeTest("_st_rt%","^.{1}st.{1}rt.*$");
    }

    @Test
    public void selectAllFromTableWithLikeQueryRange() throws ParseException {
        likeTest("st[dz]rt[a-d]time%","^st[dz]{1}rt[a-d]{1}time.*$");
    }

    private void likeTest(String like, String regex) throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where subDocument.value LIKE '"+like+"'");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("subDocument.value",document("$regex",regex)),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void countAllFromTableWithNotLikeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select count(*) from my_table where value NOT LIKE 'start%'");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$not",document("value",document("$regex","^start.*$"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithNotLikeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value NOT LIKE 'start%'");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$not",document("value",document("$regex","^start.*$"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void fuzzyDateTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where date(column,'natural') >= '5000 days ago'");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        Date resultDate = mongoDBQueryHolder.getQuery().get("column",Document.class).get("$gte",Date.class);
        DateTime fiveThousandDaysAgo = new DateTime().minusDays(5000);
        assertTrue(new Interval(fiveThousandDaysAgo.minusMinutes(5),fiveThousandDaysAgo.plusMinutes(5)).contains(new DateTime(resultDate)));
    }

    @Test
    public void fuzzyDateUnparseable() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("could not parse natural date"));
        new QueryConverter("select * from my_table where date(column,'natural') <= 'quarter hour ago'");
    }

    @Test(expected = ParseException.class)
    public void regexMatchInvalidRegex() throws ParseException {
        new QueryConverter("select * from my_table where regexMatch(column,'[') = true ");
    }


    @Test(expected = ParseException.class)
    public void regexMatchMalformed() throws ParseException {
        new QueryConverter("select * from my_table where regexMatch(column,'^[ae\"gaf]+$') = false ");
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNull() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value IS NULL");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$exists",false)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNotEquals() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value!=1");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$not",document("value",1L)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongGT() throws ParseException {
        comparisonQueriesTest(">","$gt");
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongLT() throws ParseException {
        comparisonQueriesTest("<","$lt");
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongGTE() throws ParseException {
        comparisonQueriesTest(">=","$gte");
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongLTE() throws ParseException {
        comparisonQueriesTest("<=","$lte");
    }

    private void comparisonQueriesTest(String equation, String comparisonFunction) throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value "+equation+" 1");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document(comparisonFunction,1L)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void testCountAllGroupBy() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("SELECT agent_code,   \n" +
                "COUNT (*)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$agent_code").append("count",document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("agent_code"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("agent_code",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void testCountAllGroupByMultipleFields() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("SELECT field_1, field_2,   \n" +
                "COUNT (*)   \n" +
                "FROM orders \n " +
                "WHERE field_1 LIKE 'AW_%'\n" +
                "GROUP BY field_1, field_2;");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id",new Document("field_1","$field_1").append("field_2","$field_2")).append("count",document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("field_1","field_2"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("field_1",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void countBySum() throws ParseException {
        testGroupBy("count");
    }

    @Test
    public void groupBySum() throws ParseException {
        testGroupBy("sum");
    }

    @Test
    public void groupByAvg() throws ParseException {
        testGroupBy("avg");
    }

    @Test
    public void groupByMin() throws ParseException {
        testGroupBy("min");
    }

    @Test
    public void groupByMax() throws ParseException {
        testGroupBy("max");
    }

    private void testGroupBy(String function) throws ParseException {
        QueryConverter queryConverter = new QueryConverter("SELECT agent_code,   \n" +
                function.toUpperCase()+" (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$agent_code").append(("count".equals(function) ? "count" : function + "_advance_amount"),document("$"+ ("count".equals(function) ? "sum" : function),"count".equals(function) ? 1 : "$advance_amount")),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("agent_code"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("agent_code",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereSimpleAnd() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value=1 AND value2=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$and",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereSimpleOr() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value=1 OR value2=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereNestedOr() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value=1 OR (number = 1 AND value2=\"theValue\")");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",
                    document("value",1L),
                    document("$and",
                                document("number",1L),
                                document("value2","theValue")
                            )
                ),
                mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectColumnsFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select column1, column2 from my_table where value=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1).append("column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectColumnsWithIdFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select column1, column2, _id from my_table where value=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",1).append("column1",1).append("column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectNestedColumnsFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select document.subdocument.column1, document.subdocument.column2 from my_table where value=\"theValue\"");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("document.subdocument.column1",1).append("document.subdocument.column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectWithSubQuery() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("Only column names supported"));
        new QueryConverter("select (select id from table2), column2 from my_table where value=\"theValue\"");
    }

    @Test
    public void deleteQuery() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("Only select statements are supported"));
        new QueryConverter("delete from table where value = 1");
    }

    @Test
    public void fromWithSubQuery() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("Only one simple table name is supported"));
        new QueryConverter("select column2 (select column4 from table_2) my_table where value=\"theValue\"");
    }


    @Test
    public void selectFromMultipleTables() throws ParseException {
        exception.expect(ParseException.class);
        exception.expectMessage(containsString("Only one simple table name is supported"));
        new QueryConverter("select table1.col1, table2.col2 from table1,table2 where table1.id=table2.id AND value=\"theValue\"");
    }

    @Test
    public void selectColumnsFromTableWithSimpleWhereWithInClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select column1 from my_table where value IN (\"theValue1\",\"theValue2\",\"theValue3\")");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("$in","theValue1","theValue2","theValue3"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectColumnsFromTableWithSimpleWhereWithNotInClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select column1 from my_table where value NOT IN (\"theValue1\",\"theValue2\",\"theValue3\")");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("$nin","theValue1","theValue2","theValue3"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void complicatedTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where (value=1 and  date(column,'YYY-MM-DD') <= '2016-12-12' AND nullField IS NULL ) OR ((number > 5 OR number = 1) AND value2=\"theValue\")");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(
                document("$or",
                        document("$and",
                                   document("$and",
                                           document("value",1L),
                                           document("column",document("$lte",new Date(1452556800000L)))
                                        ),
                                    document("nullField",document("$exists",false))
                                   ),
                           document("$and",
                                document("$or",
                                            document("number", document("$gt", 5L)),
                                            document("number",1L)
                                        ),
                                document("value2","theValue")
                           )

                        )
                ,
                mongoDBQueryHolder.getQuery());
    }

    @Test
    public void writeWithoutProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value IS NULL");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"value\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeSortByWithoutProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table where value IS NULL order by field_1, field_2 DESC");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"value\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "}).sort({\n" +
                "  \"field_1\": 1,\n" +
                "  \"field_2\": -1\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeWithoutDistinctProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select distinct column1 from my_table where value IS NULL");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.distinct(\"column1\" , {\n" +
                "  \"value\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeCount() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select count(*) from my_table where value IS NULL");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.count({\n" +
                "  \"value\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeSumGroupBy() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("SELECT agent_code,   \n" +
                "SUM (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.orders.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"agent_code\": {\n" +
                "      \"$regex\": \"^AW.{1}.*$\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$group\": {\n" +
                "    \"_id\": \"$agent_code\",\n" +
                "    \"sum_advance_amount\": {\n" +
                "      \"$sum\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeSumGroupByWithOptions() throws ParseException, IOException {
        System.setProperty(QueryConverter.D_AGGREGATION_ALLOW_DISK_USE,"true");
        System.setProperty(QueryConverter.D_AGGREGATION_BATCH_SIZE,"50");
        QueryConverter queryConverter = new QueryConverter("SELECT agent_code,   \n" +
                "SUM (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.orders.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"agent_code\": {\n" +
                "      \"$regex\": \"^AW.{1}.*$\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$group\": {\n" +
                "    \"_id\": \"$agent_code\",\n" +
                "    \"sum_advance_amount\": {\n" +
                "      \"$sum\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "}],{\n" +
                "  \"allowDiskUse\": true,\n" +
                "  \"cursor\": {\n" +
                "    \"batchSize\": 50\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeSumGroupByWithSort() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("SELECT agent_code,   \n" +
                "COUNT (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY COUNT (advance_amount) DESC;");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.orders.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"agent_code\": {\n" +
                "      \"$regex\": \"^AW.{1}.*$\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$group\": {\n" +
                "    \"_id\": \"$agent_code\",\n" +
                "    \"count\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"count\": -1\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeWithProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select column1, column2 from my_table where value IS NULL");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"value\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "} , {\n" +
                "  \"_id\": 0,\n" +
                "  \"column1\": 1,\n" +
                "  \"column2\": 1\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }


    private static Document document(String key, Object... values) {
        Document document = new Document();
        if (values.length > 1) {
            document.put(key,Arrays.asList(values));
        } else {
            document.put(key,values[0]);
        }
        return document;
    }

    private static Document documentValuesArray(String key, Object... values) {
        return new Document(key,Arrays.asList(values));
    }

}
