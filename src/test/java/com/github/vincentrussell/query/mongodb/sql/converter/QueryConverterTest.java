package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.print.Doc;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

public class QueryConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void selectAllFromTableWithoutWhereClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(-1,mongoDBQueryHolder.getLimit());
        assertEquals(-1,mongoDBQueryHolder.getOffset());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    public void selectFieldSurroundedInQuotes() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table WHERE \"References.field\" = \"abc123\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("References.field", "abc123"), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectFieldSurroundedInQuotesAndNumber() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table WHERE \"References.field\" = 129").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("References.field", 129L), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectFieldSurroundedInQuotesInFunctions() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from table where ignoreCaseMatch(\"field1\",\"value\") OR \"field1\" = \"value\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("$or", objsToList(documentValuesArray("$ignoreCaseMatch", "field1", "value"), document("field1", "value"))), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithoutWhereClauseLimit() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table\n" +
                "limit 10").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(10,mongoDBQueryHolder.getLimit());
        assertEquals(-1,mongoDBQueryHolder.getOffset());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }
    
    @Test
    public void selectAllFromTableWithoutWhereClauseOffset() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table\n" +
                "offset 10").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(-1,mongoDBQueryHolder.getLimit());
        assertEquals(10,mongoDBQueryHolder.getOffset());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }
    
    @Test
    public void selectAllFromTableWithoutWhereClauseLimitOffset() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table\n" +
                "limit 10 offset 10").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(10,mongoDBQueryHolder.getLimit());
        assertEquals(10,mongoDBQueryHolder.getOffset());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    public void selectAllFromTableWithoutWhereClauseOrderByField1() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table\n" +
                "order by field_1 ASC, field_2 DESC").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("field_1",1).append("field_2",-1),mongoDBQueryHolder.getSort());
        assertEquals(0,mongoDBQueryHolder.getQuery().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1")
                .fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.STRING);
        }}
        ).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value","1"),mongoDBQueryHolder.getQuery());
        assertEquals(SQLCommandType.SELECT, mongoDBQueryHolder.getSqlCommandType());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithNumber() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=\"1\"").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.NUMBER);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",1L),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithNumberNegativeLong() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=-1057614563").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.NUMBER);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",-1057614563L),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithNumberGT() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value > \"1\"").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.NUMBER);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$gt", 1L)),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithDateGT() throws ParseException, java.text.ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value > \"2012-12-01\"").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.DATE);
        }}
        ).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$gt", new SimpleDateFormat("yyyy-MM-dd").parse("2012-12-01"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void withAbsentField() throws ParseException {
        final String key  = "count";
        final String query = "select * from table where "+ key + " = 0";

        QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        Document document = mongoDBQueryHolder.getQuery();
        final Object value = document.get(key);
        assertEquals(Long.class, value.getClass());
    }

    @Test
    public void withPresentField() throws ParseException {
        final String key  = "count";
        final String query = "select * from table where "+ key + " = 0";

        QueryConverter queryConverter = new QueryConverter.Builder().sqlString(query).fieldNameToFieldTypeMapping(new HashMap(){{
            put(key, FieldType.STRING);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        Document document = mongoDBQueryHolder.getQuery();
        final Object value = document.get(key);
        assertEquals(String.class, value.getClass());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithDateISO8601GT() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value > \"2013-07-12T18:31:01.000Z\"")
                .fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.DATE);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$gt", new Date(1373653861000L))),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWithDateNaturalGT() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value > \"45 days ago\"").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.DATE);
        }}).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        Date resultDate = mongoDBQueryHolder.getQuery().get("value",Document.class).get("$gt",Date.class);
        DateTime fortyFiveDaysAgo = new DateTime().minusDays(45);
        assertTrue(new Interval(fortyFiveDaysAgo.minusMinutes(5),fortyFiveDaysAgo.plusMinutes(5)).contains(new DateTime(resultDate)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDoubleAsValue() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where avg > 1.5").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("avg", document("$gt", Double.parseDouble("1.5"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNegativeDoubleAsValue() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where avg > -1.5").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("avg", document("$gt", Double.parseDouble("-1.5"))),mongoDBQueryHolder.getQuery());
    }

    @SuppressWarnings("unchecked")
    public void testNegativePositiveAsValue() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where avg > +1.5").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("avg", document("$gt", Double.parseDouble("+1.5"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNegativeLongAsValue() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where avg > -90210").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("avg", document("$gt", Long.parseLong("-90210"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPostitiveLongAsValue() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where avg > +90210").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("avg", document("$gt", Long.parseLong("90210"))),mongoDBQueryHolder.getQuery());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testTimestampAsValue() throws Exception {
        withTimeZone(TimeZone.getTimeZone("Europe/Paris"), new MyRunnable() {
            @Override
            public void run() throws  Exception {
                QueryConverter queryConverter = new QueryConverter.Builder()
                        .sqlString("select * from my_table where date > {ts '2019-10-11 12:12:23.234'}").build();
                MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
                assertEquals(0,mongoDBQueryHolder.getProjection().size());
                assertEquals("my_table",mongoDBQueryHolder.getCollection());
                assertEquals(document("date", document("$gt", new SimpleDateFormat("E dd MMM yyyy HH:mm:ss.SSS z")
                        .parse("Fri 11 Oct 2019 12:12:23.234 CEST"))),mongoDBQueryHolder.getQuery());
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDateAsValue() throws Exception {
        withTimeZone(TimeZone.getTimeZone("Europe/Paris"), new MyRunnable() {
            @Override
            public void run() throws  Exception {
                QueryConverter queryConverter = new QueryConverter.Builder()
                        .sqlString("select * from my_table where date > {d '2019-10-11'}").build();
                MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
                assertEquals(0,mongoDBQueryHolder.getProjection().size());
                assertEquals("my_table",mongoDBQueryHolder.getCollection());
                assertEquals(document("date", document("$gt", new SimpleDateFormat("yyyy-MM-dd")
                        .parse("2019-10-11"))),mongoDBQueryHolder.getQuery());
            }
        });
    }

    private void withTimeZone(TimeZone timeZone, MyRunnable runnable) throws Exception {
        TimeZone currentTimeZone = Calendar.getInstance().getTimeZone();
        try {
            TimeZone.setDefault(timeZone);
            runnable.run();
        } finally {
            TimeZone.setDefault(currentTimeZone);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void selectAllFromTableWithSimpleWhereClauseLongOverrideWitUnparseableNaturalDateGT() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("could not convert who cares to a date"));
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value > \"who cares\"").fieldNameToFieldTypeMapping(new HashMap(){{
            put("value",FieldType.DATE);
        }}).build();
    }


    @Test
    public void selectAllFromTableWithSimpleWhereClauseLong() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",1L),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void selectDistinctFieldFromTableWithSimpleWhereClauseLong() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select DISTINCT column1 from my_table where value=1").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(1, mongoDBQueryHolder.getProjection().size());
        assertEquals(document("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals("my_table", mongoDBQueryHolder.getCollection());
        assertEquals(document("value", 1L), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectDistinctMultipleFields() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("cannot run distinct one more than one column"));
        new QueryConverter.Builder().sqlString("select DISTINCT column1, column2 from my_table where value=1").build();
    }

    @Test
    public void selectDistinctAll() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("cannot run distinct one more than one column"));
        new QueryConverter.Builder().sqlString("select DISTINCT * from my_table where value=1").build();
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNotNull() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value IS NOT NULL").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$exists",true)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatch() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'^[ae\"gaf]+$') = true ").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void notRegexMatch() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where notRegexMatch(column,'^[ae\"gaf]+$') = true ").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column", document("$not", Pattern.compile("^[ae\"gaf]+$"))).toJson(),mongoDBQueryHolder.getQuery().toJson());
    }

    @Test
    public void regexMatchWithEscapedQuote() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'^[ae\"don''tgaf]+$') = true ").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"don'tgaf]+$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatchtWithFieldMappingsOfString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where regexMatch(column,'^[ae\"gaf]+$') = true ")
                .fieldNameToFieldTypeMapping(ImmutableMap.<String,FieldType>builder()
                        .put("column",FieldType.DATE).build())
        .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatchWithOptions() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'^[ae\"gaf]+$','si') = true ").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$").append("$options","si")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void regexMatchWithoutEquals() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'^[ae\"gaf]+$')").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$regex","^[ae\"gaf]+$")),mongoDBQueryHolder.getQuery());
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where date(column,'YYYY-MM-DD') "+equation+" '2016-12-12' ").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document(mongoFunction,new Date(1452556800000L))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void dateTestWithFieldMappingsOfString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where date(column,'YYYY-MM-DD') > '2016-12-12' ")
                .fieldNameToFieldTypeMapping(ImmutableMap.<String,FieldType>builder()
                .put("column",FieldType.DATE).build()).defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("column",document("$gt",new Date(1452556800000L))),mongoDBQueryHolder.getQuery());
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

    @Test
    public void likeTestWithDoubleQuotes() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where _id LIKE \"PREFIX%\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",document("$regex","^PREFIX.*$")),mongoDBQueryHolder.getQuery());
    }

    private void likeTest(String like, String regex) throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where subDocument.value LIKE '"+like+"'").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("subDocument.value",document("$regex",regex)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void likeTestWithAbsentFieldType() throws ParseException {
        String key = "key";
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where "+ key +" = 0").build();
        Document document = queryConverter.getMongoQuery().getQuery();
        assertEquals(Long.class, document.get(key).getClass());
    }

    @Test
    public void likeTestWithDefaultFieldType() throws ParseException {
        String key = "key";
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where "+ key +" = 0").defaultFieldType(FieldType.STRING).build();
        Document document = queryConverter.getMongoQuery().getQuery();
        assertEquals("0", document.get(key));
        assertEquals(String.class, document.get(key).getClass());
    }

    @Test
    public void countAllFromTableWithNotLikeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) from my_table where value NOT LIKE 'start%'").build();
        Document document = queryConverter.getMongoQuery().getQuery();
        assertEquals(document("value", document("$not", Pattern.compile("^start.*$"))).toJson(), document.toJson());
    }

    @Test
    public void selectAllFromTableWithNotLikeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value NOT LIKE 'start%'").build();
        Document document = queryConverter.getMongoQuery().getQuery();
        assertEquals(document("value", document("$not", Pattern.compile("^start.*$"))).toJson(), document.toJson());
    }

    @Test
    public void fuzzyDateTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where date(column,'natural') >= '5000 days ago'").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        Date resultDate = mongoDBQueryHolder.getQuery().get("column",Document.class).get("$gte",Date.class);
        DateTime fiveThousandDaysAgo = new DateTime().minusDays(5000);
        assertTrue(new Interval(fiveThousandDaysAgo.minusMinutes(5),fiveThousandDaysAgo.plusMinutes(5)).contains(new DateTime(resultDate)));
    }

    @Test
    public void fuzzyDateUnparseable() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("could not natural language date: quarter hour ago"));
        new QueryConverter.Builder().sqlString("select * from my_table where date(column,'natural') <= 'quarter hour ago'").build();
    }

    @Test(expected = ParseException.class)
    public void regexMatchInvalidRegex() throws ParseException {
        new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'[') = true ").build();
    }


    @Test(expected = ParseException.class)
    public void regexMatchMalformed() throws ParseException {
       QueryConverter queryConverter =  new QueryConverter.Builder().sqlString("select * from my_table where regexMatch(column,'^[ae\"gaf]+$') = false ").build();
       assertNull(queryConverter);
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNull() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value IS NULL").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document("$exists",false)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where QUICKSEARCH('123') AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$QUICKSEARCH", "123"), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionRecursiveTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where toLower(toUpper('123')) AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$toLower", document("$toUpper", "123")), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionWithEqualsTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where someFunction('123') = \"1234\" AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", documentValuesArray("$eq", document("$someFunction", "123"), "1234"), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionWithInTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where someFunction('field') IN (\"1234\") AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$fin", document("function", document("$someFunction", "field")).append("list", Lists.newArrayList("1234"))), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionWithNotInTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where someFunction('field') NOT IN (\"1234\") AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$fnin", document("function", document("$someFunction", "field")).append("list", Lists.newArrayList("1234"))), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void objectIdFunctionTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where OBJECTID('_id') = '53102b43bf1044ed8b0ba36b' AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("_id", new ObjectId("53102b43bf1044ed8b0ba36b")), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }
    
    @Test
    public void OIDInsideEqualTest() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select t._id as id, t.Restaurant as R from Restaurants as t  where t._id = OID('5e97ae59c63d1b3ff8e07c74')").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"_id\": {\n" +
                "      \"$oid\": \"5e97ae59c63d1b3ff8e07c74\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"_id\": 0,\n" +
                "    \"id\": \"$_id\",\n" +
                "    \"R\": \"$Restaurant\"\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void getQueryAsDocumentAggregation() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select t._id as id, t.Restaurant as R from Restaurants as t  where t._id = OID('5e97ae59c63d1b3ff8e07c74')").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Document document = queryConverter.getQueryAsDocument();
        assertEquals("Restaurants", document.getString("collection"));
        assertNotNull(document.getList("query", Document.class));
    }

    @Test
    public void getQueryAsDocumentCountAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where value=1 OR value2=\"theValue\"").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Document document = queryConverter.getQueryAsDocument();
        assertEquals("my_table", document.getString("collection"));
        assertTrue(Document.class.isInstance(document.get("query")));
        assertEquals(SQLCommandType.SELECT.name(), document.get("commandType"));
    }

    @Test
    public void objectIdFunctionNotTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where OBJECTID('_id') != '53102b43bf1044ed8b0ba36b' AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("_id", document("$ne", new ObjectId("53102b43bf1044ed8b0ba36b"))), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void objectIdFunctionInTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where OBJECTID('_id') IN ('53102b43bf1044ed8b0ba36b', '54651022bffebc03098b4568') AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("_id", document("$in", Lists.newArrayList(new ObjectId("53102b43bf1044ed8b0ba36b"), new ObjectId("54651022bffebc03098b4568")))), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }
    
    @Test
    public void OIDInsideInTest() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select t._id as id, t.Restaurant as R from Restaurants as t  where t._id IN (OID('5e97ae59c63d1b3ff8e07c74'),OID('5e97ae58c63d1b3ff8e07c73'),OID('5e97ae58c63d1b3ff8e07c72'),OID('5e97ae58c63d1b3ff8e07c71'),OID('5e97ae58c63d1b3ff8e07c70'))").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"_id\": {\n" +
                "      \"$in\": [\n" +
                "        {\n" +
                "          \"$oid\": \"5e97ae59c63d1b3ff8e07c74\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"$oid\": \"5e97ae58c63d1b3ff8e07c73\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"$oid\": \"5e97ae58c63d1b3ff8e07c72\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"$oid\": \"5e97ae58c63d1b3ff8e07c71\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"$oid\": \"5e97ae58c63d1b3ff8e07c70\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"_id\": 0,\n" +
                "    \"id\": \"$_id\",\n" +
                "    \"R\": \"$Restaurant\"\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void objectIdFunctionNotInTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where OBJECTID('_id') NOT IN ('53102b43bf1044ed8b0ba36b', '54651022bffebc03098b4568') AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("_id", document("$nin", Lists.newArrayList(new ObjectId("53102b43bf1044ed8b0ba36b"), new ObjectId("54651022bffebc03098b4568")))), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionWithNoArgsTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where QUICKSEARCH() AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$QUICKSEARCH", null), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void specialtyFunctionWithMultipleArgsTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select * from my_table where QUICKSEARCH(123, \"123\") AND (foo = 'bar')")
                .defaultFieldType(FieldType.STRING).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(documentValuesArray("$and", document("$QUICKSEARCH", Lists.newArrayList(123L, "123")), document("foo", "bar") ), mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseLongNotEquals() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value!=1").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value", document("$ne", 1L)), mongoDBQueryHolder.getQuery());
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value "+equation+" 1").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value",document(comparisonFunction,1L)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void testCountAllGroupBy() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                "COUNT (*)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$agent_code").append("count",document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("agent_code"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("agent_code",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void testCountAllGroupByMultipleFields() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT field_1, field_2,   \n" +
                "COUNT (*)   \n" +
                "FROM orders \n " +
                "WHERE field_1 LIKE 'AW_%'\n" +
                "GROUP BY field_1, field_2;").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                function.toUpperCase()+" (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$agent_code").append(("count".equals(function) ? "count" : function + "_advance_amount"),document("$"+ ("count".equals(function) ? "sum" : function),"count".equals(function) ? 1 : "$advance_amount")),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("agent_code"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("agent_code",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereSimpleAnd() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1 AND value2=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$and",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereSimpleOr() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1 OR value2=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereNotBeforeBraces() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where NOT (value=\"theValue\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$nor", Lists.newArrayList(document("value","theValue"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereNotBeforeAndInBraces() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where NOT (value=1 AND value2=\"theValue\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$nor", Lists.newArrayList(document("$and",document("value",1L),document("value2","theValue")))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereNotBeforeOrInBraces() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where NOT (value=1 OR value2=\"theValue\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$nor", Lists.newArrayList(document("$or",document("value",1L),document("value2","theValue")))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllFromTableWithSimpleWhereNestedOr() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1 OR (number = 1 AND value2=\"theValue\")").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1, column2 from my_table where value=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1).append("column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectColumnsWithIdFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1, column2, _id from my_table where value=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",1).append("column1",1).append("column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectAllTwoColumns() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table WHERE _id = index_9").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id","index_9"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectNestedColumnsFromTableWithSimpleWhereClauseString() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select document.subdocument.column1, document.subdocument.column2 from my_table where value=\"theValue\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(3,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("document.subdocument.column1",1).append("document.subdocument.column2",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value","theValue"),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectWithSubQuery() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("Unsupported subselect expression"));
        new QueryConverter.Builder().sqlString("select (select id from table2), column2 from my_table where value=\"theValue\"").build();
    }

    @Test
    public void multipleNotStatements() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT * FROM Customers\n" +
                "WHERE NOT (Country='Germany' AND City='Berlin') and NOT (Country='Mexico');").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(documentValuesArray("$and",
               documentValuesArray("$nor", documentValuesArray("$and", document("Country", "Germany"), document("City", "Berlin"))),
                documentValuesArray("$nor", document("Country", "Mexico"))),mongoDBQueryHolder.getQuery());
        assertEquals(SQLCommandType.SELECT, mongoDBQueryHolder.getSqlCommandType());
    }

    @Test
    public void deleteQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("delete from table where value = 1").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("value",1L),mongoDBQueryHolder.getQuery());
        assertEquals(SQLCommandType.DELETE, mongoDBQueryHolder.getSqlCommandType());
    }

    @Test
    public void deleteQueryMoreComplicated() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("delete from table where value IN (\"theValue1\",\"theValue2\",\"theValue3\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("$expr",documentValuesArray("$in","$value",objsToList("theValue1","theValue2","theValue3"))),mongoDBQueryHolder.getQuery());
        assertEquals(SQLCommandType.DELETE, mongoDBQueryHolder.getSqlCommandType());
    }
    
    @Test
    public void deleteInQuery() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("delete from Restaurants where Restaurant.cuisine IN ('American ','Italian','Chinese')").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.remove({\n" + 
        		"  \"$expr\": {\n" + 
        		"    \"$in\": [\n" + 
        		"      \"$Restaurant.cuisine\",\n" + 
        		"      [\n" + 
        		"        \"American \",\n" + 
        		"        \"Italian\",\n" + 
        		"        \"Chinese\"\n" + 
        		"      ]\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void fromWithSubQuery() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("Only one simple table name is supported."));
        new QueryConverter.Builder().sqlString("select column2 (select column4 from table_2) my_table where value=\"theValue\"").build();
    }


    @Test
    public void selectFromMultipleTables() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage(containsString("Join type not supported"));
        new QueryConverter.Builder().sqlString("select table1.col1, table2.col2 from table1,table2 where table1.id=table2.id AND value=\"theValue\"").build();
    }

    @Test
    public void selectColumnsFromTableWithSimpleWhereWithInClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 from my_table where value IN (\"theValue1\",\"theValue2\",\"theValue3\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals((document("value", documentValuesArray("$in","theValue1","theValue2","theValue3"))),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void selectColumnsFromTableWithSimpleWhereWithNotInClause() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 from my_table where value NOT IN (\"theValue1\",\"theValue2\",\"theValue3\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("_id",0).append("column1",1),mongoDBQueryHolder.getProjection());
        assertEquals(document("value",documentValuesArray("$nin", "theValue1","theValue2","theValue3")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void complicatedTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where (value=1 and  date(column,'YYYY-MM-DD') <= '2016-12-12' AND nullField IS NULL ) OR ((number > 5 OR number = 1) AND value2=\"theValue\")").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());

        assertEquals(document("$or", Lists.newArrayList(
                document("$and", Lists.newArrayList(
                        document("value",1L),
                        document("column",document("$lte",new Date(1452556800000L))),
                        document("nullField",document("$exists",false))
                )),
                document("$and", Lists.newArrayList(
                        document("$or", Lists.newArrayList(
                                document("number", document("$gt", 5L)),
                                document("number",1L)
                        )),
                        document("value2","theValue")
                ))
        )), mongoDBQueryHolder.getQuery());
    }




    @Test
    public void aLotofOrs() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where (value = \"1234\" OR value = \"1235\" OR value = \"1236\" OR value = \"1237\"  OR value = \"1238\")").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"$or\": [\n" +
                "    {\n" +
                "      \"value\": \"1234\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1235\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1236\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1237\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1238\"\n" +
                "    }\n" +
                "  ]\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }


    @Test
    public void aLotofAnds() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where (value = \"1234\" AND value = \"1235\" AND value = \"1236\" AND value = \"1237\"  AND value = \"1238\")").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"$and\": [\n" +
                "    {\n" +
                "      \"value\": \"1234\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1235\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1236\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1237\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"1238\"\n" +
                "    }\n" +
                "  ]\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void doubleQuotesAreRemoved() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where \"foo\" IS NULL").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.find({\n" +
                "  \"foo\": {\n" +
                "    \"$exists\": false\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeWithoutProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value IS NULL").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value IS NULL order by field_1, field_2 DESC").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select distinct column1 from my_table where value IS NULL").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) from my_table where value IS NULL").build();
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
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                "SUM (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;").build();
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
                "},{\n" +
				"  \"$project\": {\n" +
				"    \"agent_code\": \"$_id\",\n" +
				"    \"sum_advance_amount\": 1,\n" +
				"    \"_id\": 0\n" +
				"  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeSumGroupByWithOptions() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                "SUM (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code;")
                .aggregationAllowDiskUse(true)
                .aggregationBatchSize(50)
                .build();
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
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"agent_code\": \"$_id\",\n" +
                "    \"sum_advance_amount\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}],{\n" +
                "  \"allowDiskUse\": true,\n" +
                "  \"cursor\": {\n" +
                "    \"batchSize\": 50\n" +
                "  }\n" +
                "})",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeCountGroupByWithSort() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                "COUNT (advance_amount)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY COUNT (advance_amount) DESC;").build();
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
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"agent_code\": \"$_id\",\n" +
                "    \"count\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeCountGroupByWithSortWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "COUNT (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY COUNT (advance_amount) DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSumGroupByWithSortWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "SUM (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY SUM (advance_amount) DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$sum\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSumGroupByWithSortAliasWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "SUM (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY c DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$sum\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeMaxGroupByWithSortWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "MAX (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY MAX (advance_amount) DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$max\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeMinGroupByWithSortWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "MIN (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY MIN (advance_amount) DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$min\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeAvgGroupByWithSortWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac,   \n" +
                "AVG (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code\n" +
                "ORDER BY AVG (advance_amount) DESC;").build();
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
                "    \"c\": {\n" +
                "      \"$avg\": \"$advance_amount\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"c\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeCountGroupByWithSortCountWithMultiAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac, city_code as cc,  \n" +
                "COUNT (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code, city_code\n" +
                "ORDER BY COUNT (advance_amount) DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
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
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"cc\": \"$_id.city_code\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeCountGroupByWithSortFieldsWithMultiAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac, city_code as cc,  \n" +
                "COUNT (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code, city_code\n" +
                "ORDER BY agent_code asc, city_code DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
                "    },\n" +
                "    \"c\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.agent_code\": 1,\n" +
                "    \"_id.city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"cc\": \"$_id.city_code\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeCountGroupByWithSortAliasFieldsWithMultiAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac, city_code as cc,  \n" +
                "COUNT (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code, city_code\n" +
                "ORDER BY ac asc, cc DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
                "    },\n" +
                "    \"c\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.agent_code\": 1,\n" +
                "    \"_id.city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"cc\": \"$_id.city_code\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeCountGroupByWithSortFieldsWithPartialAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac, city_code,  \n" +
                "COUNT (advance_amount) as c  \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code, city_code\n" +
                "ORDER BY agent_code asc, city_code DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
                "    },\n" +
                "    \"c\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.agent_code\": 1,\n" +
                "    \"_id.city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"city_code\": \"$_id.city_code\",\n" +
                "    \"c\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }

    
    @Test
    public void writeCountGroupByWithSortFieldsWithPartialAliasNoCount() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code as ac, city_code,  \n" +
                "COUNT (advance_amount) \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code, city_code\n" +
                "ORDER BY agent_code asc, city_code DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
                "    },\n" +
                "    \"count\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.agent_code\": 1,\n" +
                "    \"_id.city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"city_code\": \"$_id.city_code\",\n" +
                "    \"count\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjections() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1, column2 from my_table where value IS NULL").build();
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
    
    @Test
    public void writeWithProjectionsAliasSingle() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 from my_table where value IS NULL").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 as c2 from my_table where value IS NULL").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortSingle() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 from my_table where value IS NULL order by column1 asc").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortMixed() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 from my_table where value IS NULL order by column1 asc, column2").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }

    @Test
    public void writeWithProjectionsAliasAllSortAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 as c2 from my_table where value IS NULL order by column1 asc, column2 asc").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortAllLimit() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 as c2 from my_table where value IS NULL order by column1 asc, column2 asc limit 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$limit\": 3\n" +
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" +
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortAllOffset() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 as c2 from my_table where value IS NULL order by column1 asc, column2 asc offset 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$skip\": 3\n" +
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" +
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortAllLimitOffset() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select column1 as c1, column2 as c2 from my_table where value IS NULL order by column1 asc, column2 asc limit 4 offset 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$skip\": 3\n" +
        		"},{\n" + 
        		"  \"$limit\": 4\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" +
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeWithProjectionsTableAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.column1, c.column2 from my_table as c where c.value IS NULL").build();
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
    
    @Test
    public void writeWithProjectionsAliasSingleTableAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.column1 as c1, c.column2 from my_table as c where c.value IS NULL").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    
    @Test
    public void writeWithProjectionsAliasAllSortAllLimitOffsetTableAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.column1 as c1, c.column2 as c2 from my_table as c where c.value IS NULL order by c.column1 asc, c.column2 asc limit 4 offset 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"column1\": 1,\n" + 
        		"    \"column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$skip\": 3\n" +
        		"},{\n" + 
        		"  \"$limit\": 4\n" +
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$column1\",\n" + 
        		"    \"c2\": \"$column2\"\n" + 
        		"  }\n" +
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeWithProjectionsAliasAllSortAllLimitOffsetTableAliasNestedFields() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.sub1.column1 as c1, c.sub2.column2 as c2 from my_table as c where c.value IS NULL order by c.sub1.column1 asc, c.sub2.column2 asc limit 4 offset 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.my_table.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"value\": {\n" + 
        		"      \"$exists\": false\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"sub1.column1\": 1,\n" + 
        		"    \"sub2.column2\": 1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$skip\": 3\n" +
        		"},{\n" + 
        		"  \"$limit\": 4\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"_id\": 0,\n" + 
        		"    \"c1\": \"$sub1.column1\",\n" + 
        		"    \"c2\": \"$sub2.column2\"\n" + 
        		"  }\n" +
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSumGroupByWithSortFieldsWithPartialAliasNoCountTableAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT c.agent_code as ac, c.city_code,  \n" +
                "COUNT (c.advance_amount) \n" +
                "FROM orders as c\n " +
                "WHERE c.agent_code LIKE 'AW_%'\n" +
                "GROUP BY c.agent_code, c.city_code\n" +
                "ORDER BY c.agent_code asc, c.city_code DESC;").build();
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
                "    \"_id\": {\n" + 
                "      \"agent_code\": \"$agent_code\",\n" +
                "      \"city_code\": \"$city_code\"\n" +
                "    },\n" +
                "    \"count\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.agent_code\": 1,\n" +
                "    \"_id.city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.agent_code\",\n" +
                "    \"city_code\": \"$_id.city_code\",\n" +
                "    \"count\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeSumGroupByWithSortFieldsWithPartialAliasNoCountTableAliasNestedFields() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT c.sub1.agent_code as ac, c.sub2.city_code,  \n" +
                "COUNT (c.advance_amount) \n" +
                "FROM orders as c\n " +
                "WHERE c.sub1.agent_code LIKE 'AW_%'\n" +
                "GROUP BY c.sub1.agent_code, c.sub2.city_code\n" +
                "ORDER BY c.sub1.agent_code asc, c.sub2.city_code DESC;").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.orders.aggregate([{\n" +
                "  \"$match\": {\n" +
                "    \"sub1.agent_code\": {\n" +
                "      \"$regex\": \"^AW.{1}.*$\"\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$group\": {\n" +
                "    \"_id\": {\n" + 
                "      \"sub1_agent_code\": \"$sub1.agent_code\",\n" +
                "      \"sub2_city_code\": \"$sub2.city_code\"\n" +
                "    },\n" +
                "    \"count\": {\n" +
                "      \"$sum\": 1\n" +
                "    }\n" +
                "  }\n" +
                "},{\n" +
                "  \"$sort\": {\n" +
                "    \"_id.sub1_agent_code\": 1,\n" +
                "    \"_id.sub2_city_code\": -1\n" +
                "  }\n" +
                "},{\n" +
                "  \"$project\": {\n" +
                "    \"ac\": \"$_id.sub1_agent_code\",\n" +
                "    \"sub2.city_code\": \"$_id.sub2_city_code\",\n" +
                "    \"count\": 1,\n" +
                "    \"_id\": 0\n" +
                "  }\n" +
                "}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingCount() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) from Restaurants group by Restaurant.cuisine having count(*) > 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"count\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$gt\": [\n" + 
        		"        \"$count\",\n" + 
        		"        3\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"count\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingCountAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) as c from Restaurants group by Restaurant.cuisine having count(*) > 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$gt\": [\n" + 
        		"        \"$c\",\n" + 
        		"        3\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingAliasCountAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) as c from Restaurants group by Restaurant.cuisine having c > 3").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"c\": {\n" + 
        		"      \"$gt\": 3\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingFunctionCompCountAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) as c from Restaurants group by Restaurant.cuisine having count(*) = count(*)").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$eq\": [\n" + 
        		"        \"$c\",\n" + 
        		"        \"$c\"\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingFunctionCompCountAliasAndDummyWhere() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) as c from Restaurants where 1=1 group by Restaurant.cuisine having count(*) = count(*)").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$eq\": [\n" + 
        		"        1,\n" + 
        		"        1\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$eq\": [\n" + 
        		"        \"$c\",\n" + 
        		"        \"$c\"\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingMultiCondCountAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select Restaurant.cuisine, count(*) as c from Restaurants where 1=1 group by Restaurant.cuisine having count(*) >= 4 and count(*) <= 7").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$eq\": [\n" + 
        		"        1,\n" + 
        		"        1\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": \"$Restaurant.cuisine\",\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$and\": [\n" + 
        		"      {\n" + 
        		"        \"$expr\": {\n" + 
        		"          \"$gte\": [\n" + 
        		"            \"$c\",\n" + 
        		"            4\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      },\n" + 
        		"      {\n" + 
        		"        \"$expr\": {\n" + 
        		"          \"$lte\": [\n" + 
        		"            \"$c\",\n" + 
        		"            7\n" + 
        		"          ]\n" + 
        		"        }\n" + 
        		"      }\n" + 
        		"    ]\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"Restaurant.cuisine\": \"$_id\",\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingMultiOp() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select idAdaptador, process_date, avg(totalLoaded), min(structural), max(integrity), sum(business) from errorsOnDate group by idApaptador, process_date having avg(totalLoaded) > min(structural)").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.errorsOnDate.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"idAdaptador\": \"$idAdaptador\",\n" + 
        		"      \"process_date\": \"$process_date\"\n" + 
        		"    },\n" + 
        		"    \"avg_totalLoaded\": {\n" + 
        		"      \"$avg\": \"$totalLoaded\"\n" + 
        		"    },\n" + 
        		"    \"min_structural\": {\n" + 
        		"      \"$min\": \"$structural\"\n" + 
        		"    },\n" + 
        		"    \"max_integrity\": {\n" + 
        		"      \"$max\": \"$integrity\"\n" + 
        		"    },\n" + 
        		"    \"sum_business\": {\n" + 
        		"      \"$sum\": \"$business\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$gt\": [\n" + 
        		"        \"$avg_totalLoaded\",\n" + 
        		"        \"$min_structural\"\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"idAdaptador\": \"$_id.idAdaptador\",\n" + 
        		"    \"process_date\": \"$_id.process_date\",\n" + 
        		"    \"avg_totalLoaded\": 1,\n" + 
        		"    \"min_structural\": 1,\n" + 
        		"    \"max_integrity\": 1,\n" + 
        		"    \"sum_business\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingMultiOpAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select idAdaptador, process_date, avg(totalLoaded) as avgg, min(structural) as minn, max(integrity) as maxx, sum(business) as summ from errorsOnDate group by idApaptador, process_date having avgg > minn").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.errorsOnDate.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"idAdaptador\": \"$idAdaptador\",\n" + 
        		"      \"process_date\": \"$process_date\"\n" + 
        		"    },\n" + 
        		"    \"avgg\": {\n" + 
        		"      \"$avg\": \"$totalLoaded\"\n" + 
        		"    },\n" + 
        		"    \"minn\": {\n" + 
        		"      \"$min\": \"$structural\"\n" + 
        		"    },\n" + 
        		"    \"maxx\": {\n" + 
        		"      \"$max\": \"$integrity\"\n" + 
        		"    },\n" + 
        		"    \"summ\": {\n" + 
        		"      \"$sum\": \"$business\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$gt\": [\n" + 
        		"        \"$avgg\",\n" + 
        		"        \"$minn\"\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"idAdaptador\": \"$_id.idAdaptador\",\n" + 
        		"    \"process_date\": \"$_id.process_date\",\n" + 
        		"    \"avgg\": 1,\n" + 
        		"    \"minn\": 1,\n" + 
        		"    \"maxx\": 1,\n" + 
        		"    \"summ\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void writeHavingMultiOpAliasSort() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select idAdaptador, process_date, avg(totalLoaded) as avgg, min(structural) as minn, max(integrity) as maxx, sum(business) as summ from errorsOnDate group by idApaptador, process_date having avgg > minn order by avgg desc").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.errorsOnDate.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {\n" + 
        		"      \"idAdaptador\": \"$idAdaptador\",\n" + 
        		"      \"process_date\": \"$process_date\"\n" + 
        		"    },\n" + 
        		"    \"avgg\": {\n" + 
        		"      \"$avg\": \"$totalLoaded\"\n" + 
        		"    },\n" + 
        		"    \"minn\": {\n" + 
        		"      \"$min\": \"$structural\"\n" + 
        		"    },\n" + 
        		"    \"maxx\": {\n" + 
        		"      \"$max\": \"$integrity\"\n" + 
        		"    },\n" + 
        		"    \"summ\": {\n" + 
        		"      \"$sum\": \"$business\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$match\": {\n" + 
        		"    \"$expr\": {\n" + 
        		"      \"$gt\": [\n" + 
        		"        \"$avgg\",\n" + 
        		"        \"$minn\"\n" + 
        		"      ]\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$sort\": {\n" + 
        		"    \"avgg\": -1\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"idAdaptador\": \"$_id.idAdaptador\",\n" + 
        		"    \"process_date\": \"$_id.process_date\",\n" + 
        		"    \"avgg\": 1,\n" + 
        		"    \"minn\": 1,\n" + 
        		"    \"maxx\": 1,\n" + 
        		"    \"summ\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void countAllWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) as c from Restaurants").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.Restaurants.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": 1\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void sumAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select sum(Helsinki.population) from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"sum_Helsinki_population\": {\n" + 
        		"      \"$sum\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"sum_Helsinki_population\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void sumAllWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select sum(Helsinki.population) as c from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"c\": {\n" + 
        		"      \"$sum\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void minAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select min(Helsinki.population) from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"min_Helsinki_population\": {\n" + 
        		"      \"$min\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"min_Helsinki_population\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void minAllWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select min(Helsinki.population) as c from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"c\": {\n" + 
        		"      \"$min\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void maxAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select max(Helsinki.population) from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"max_Helsinki_population\": {\n" + 
        		"      \"$max\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"max_Helsinki_population\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void maxAllWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select max(Helsinki.population) as c from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"c\": {\n" + 
        		"      \"$max\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void avgAll() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select avg(Helsinki.population) from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"avg_Helsinki_population\": {\n" + 
        		"      \"$avg\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"avg_Helsinki_population\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void avgAllWithAlias() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select avg(Helsinki.population) as c from HelsinkiPopulation").build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        assertEquals("db.HelsinkiPopulation.aggregate([{\n" + 
        		"  \"$group\": {\n" + 
        		"    \"_id\": {},\n" + 
        		"    \"c\": {\n" + 
        		"      \"$avg\": \"$Helsinki.population\"\n" + 
        		"    }\n" + 
        		"  }\n" + 
        		"},{\n" + 
        		"  \"$project\": {\n" + 
        		"    \"c\": 1,\n" + 
        		"    \"_id\": 0\n" + 
        		"  }\n" + 
        		"}])",byteArrayOutputStream.toString("UTF-8"));
    }
    
    @Test
    public void doubleEquals() throws ParseException {
        expectedException.expect(ParseException.class);
        expectedException.expectMessage("unable to parse complete sql string. one reason for this is the use of double equals (==).");
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where key == 'value1'").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void singleQuoteSelect() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1 OR value2='theValue'").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",document("value",1L),document("value2","theValue")),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void unicodeCharacters() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where value=1 OR value2=\"亀a亁b亂c亃d亄\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$or",document("value",1L),document("value2","亀a亁b亂c亃d亄")),mongoDBQueryHolder.getQuery());
    }


    @Test
    public void booleanTestWithoutEquals() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where booleanField").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",true),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void negativebooleanTestWithoutEquals() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where NOT booleanField").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",document("$ne", true)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void booleanTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where booleanField = true").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",true),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void negativebooleanTest() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where booleanField != true").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",document("$ne", true)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void booleanTestFalse() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where booleanField = false").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",false),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void negativebooleanTestFalse() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where booleanField != false").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("booleanField",document("$ne", false)),mongoDBQueryHolder.getQuery());
    }

    @Test
    public void deepNestedQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from my_table where a.b.c.d.e.key = \"value\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(0,mongoDBQueryHolder.getProjection().size());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("a.b.c.d.e.key", "value"),mongoDBQueryHolder.getQuery());
    }
    
    @Test
    public void aliasPlainQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select aa as bb, cc from my_table where aa = \"value\" and cc = \"value\"").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(document("_id",0).append("bb","$aa").append("cc",1),mongoDBQueryHolder.getProjection());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("$and",document("aa","value"),document("cc","value")),mongoDBQueryHolder.getQuery());
    }
    
    @Test
    public void aliasGroupQuerySingleGroup() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select aa as bb, count(*) as dd from my_table where aa = \"value\" group by aa").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$aa").append("dd", document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals(3,mongoDBQueryHolder.getAliasProjection().size());
        assertEquals(document("bb","$_id").append("_id", 0).append("dd", 1),mongoDBQueryHolder.getAliasProjection());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("aa","value"),mongoDBQueryHolder.getQuery());
        assertEquals(Arrays.asList(new String[]{"aa"}),mongoDBQueryHolder.getGroupBys());
    }
    
    @Test
    public void aliasGroupQueryAliasAndNot() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select aa as bb, cc, count(*) as dd from my_table where aa = \"value\" group by aa, cc").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id",document("aa","$aa").append("cc","$cc")).append("dd", document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals(4,mongoDBQueryHolder.getAliasProjection().size());
        assertEquals(document("bb","$_id.aa").append("cc", "$_id.cc").append("_id", 0).append("dd", 1),mongoDBQueryHolder.getAliasProjection());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("aa","value"),mongoDBQueryHolder.getQuery());
        assertEquals(Arrays.asList(new String[]{"aa","cc"}),mongoDBQueryHolder.getGroupBys());
    }
    
    @Test
    public void aliasGroupQueryNoGroupAlias() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select aa as bb, cc, count(*) from my_table where aa = \"value\" group by aa, cc").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id",document("aa","$aa").append("cc","$cc")).append("count", document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals(4,mongoDBQueryHolder.getAliasProjection().size());
        assertEquals(document("bb","$_id.aa").append("cc", "$_id.cc").append("_id", 0).append("count", 1),mongoDBQueryHolder.getAliasProjection());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("aa","value"),mongoDBQueryHolder.getQuery());
        assertEquals(Arrays.asList(new String[]{"aa","cc"}),mongoDBQueryHolder.getGroupBys());
    }
    
    @Test
    public void aliasGroupQueryAllAlias() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select aa as bb, cc as dd, count(*) as ee from my_table where aa = \"value\" group by aa, cc").build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id",document("aa","$aa").append("cc","$cc")).append("ee", document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals(4,mongoDBQueryHolder.getAliasProjection().size());
        assertEquals(document("bb","$_id.aa").append("dd", "$_id.cc").append("_id", 0).append("ee", 1),mongoDBQueryHolder.getAliasProjection());
        assertEquals("my_table",mongoDBQueryHolder.getCollection());
        assertEquals(document("aa","value"),mongoDBQueryHolder.getQuery());
        assertEquals(Arrays.asList(new String[]{"aa","cc"}),mongoDBQueryHolder.getGroupBys());
    }
    
    @Test
    public void testCountAllGroupByWithHaving() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("SELECT agent_code,   \n" +
                "COUNT (*)   \n" +
                "FROM orders \n " +
                "WHERE agent_code LIKE 'AW_%'\n" +
                "GROUP BY agent_code \n" +
                "HAVING count > 1;"
                ).build();
        MongoDBQueryHolder mongoDBQueryHolder = queryConverter.getMongoQuery();
        assertEquals(2,mongoDBQueryHolder.getProjection().size());
        assertEquals(document("_id","$agent_code").append("count",document("$sum",1)),mongoDBQueryHolder.getProjection());
        assertEquals("orders",mongoDBQueryHolder.getCollection());
        assertEquals(Arrays.asList("agent_code"),mongoDBQueryHolder.getGroupBys());
        assertEquals(document("count",document("$gt",1L)),mongoDBQueryHolder.getHaving());
        assertEquals(document("agent_code",document("$regex","^AW.{1}.*$")),mongoDBQueryHolder.getQuery());
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
    
    private static List<Object> objsToList(Object... values) {
        return Arrays.asList(values);
    }

    public interface MyRunnable {
        void run() throws Exception;
    }

}
