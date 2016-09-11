package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Date;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

public class QueryConverterTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void selectAllFromTableWithoutWhereClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from my_table");
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
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
