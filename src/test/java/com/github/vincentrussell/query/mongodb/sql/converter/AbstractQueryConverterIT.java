package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.rule.MongoRule;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.commons.io.IOUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class AbstractQueryConverterIT {

    private static final int TOTAL_TEST_RECORDS_PRIMER = 25359;
    private static final int TOTAL_TEST_RECORDS_CUSTOMERS = 599;
    private static final int TOTAL_TEST_RECORDS_FILMS = 1000;
    private static final int TOTAL_TEST_RECORDS_STORES = 2;

    private static final String DATABASE = "local";
    protected static final String COLLECTION = "my_collection";
    protected static final String COLLECTION_CUSTOMERS = "my_collection_customers";
    protected static final String COLLECTION_FILMS = "my_collection_films";
    protected static final String COLLECTION_STORES = "my_collection_stores";
    private static final String DATASET_PRIMER = "primer-dataset";
    private static final String DATASET_FILMS = "films";
    private static final String DATASET_CUSTOMERS = "customers";
    private static final String DATASET_STORES = "stores";

    protected static JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).indentCharacters("\t").newLineCharacters("\n").build();
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<Document> mongoCollection;

    public abstract MongoRule getMongoRule();

    private static void loadRecords(int totalRecords, String dataset, MongoCollection mc) throws IOException {
    	List<Document> documents = new ArrayList<>(totalRecords);
        try(InputStream inputStream = AbstractQueryConverterIT.class.getResourceAsStream("/" + dataset + ".json");
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            while ((line = bufferedReader.readLine())!=null) {
                documents.add(Document.parse(line));
            }
        }

        for (Iterator<List<WriteModel>> iterator = Iterables.partition(Lists.transform(documents, new Function<Document, WriteModel>() {
            @Override
            public WriteModel apply(Document document) {
                return new InsertOneModel(document);
            }
        }),10000).iterator(); iterator.hasNext();) {
            mc.bulkWrite(iterator.next());
        }
    }
    
    @Before
    public void before() throws IOException {

        mongoDatabase = getMongoRule().getDatabase(DATABASE);

        mongoCollection = mongoDatabase.getCollection(COLLECTION_CUSTOMERS);

        mongoCollection.deleteMany(new Document());
        loadRecords(TOTAL_TEST_RECORDS_PRIMER,DATASET_CUSTOMERS,mongoCollection);
        assertEquals(TOTAL_TEST_RECORDS_CUSTOMERS,mongoCollection.countDocuments());

        mongoCollection = mongoDatabase.getCollection(COLLECTION_FILMS);

        mongoCollection.deleteMany(new Document());
        loadRecords(TOTAL_TEST_RECORDS_PRIMER,DATASET_FILMS,mongoCollection);
        assertEquals(TOTAL_TEST_RECORDS_FILMS,mongoCollection.countDocuments());

        mongoCollection = mongoDatabase.getCollection(COLLECTION_STORES);

        mongoCollection.deleteMany(new Document());
        loadRecords(TOTAL_TEST_RECORDS_PRIMER,DATASET_STORES,mongoCollection);
        assertEquals(TOTAL_TEST_RECORDS_STORES,mongoCollection.countDocuments());

        mongoCollection = mongoDatabase.getCollection(COLLECTION);

        mongoCollection.deleteMany(new Document());
        loadRecords(TOTAL_TEST_RECORDS_PRIMER,DATASET_PRIMER,mongoCollection);
        assertEquals(TOTAL_TEST_RECORDS_PRIMER,mongoCollection.countDocuments());
        
    }


    @Test
    public void likeQuery() throws ParseException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where address.street LIKE '%Street'").build();
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        Document firstDocument = documents.get(0);
        firstDocument.remove("_id");
        JSONAssert.assertEquals("{\n" +
                "\t\"address\": {\n" +
                "\t\t\"building\": \"351\",\n" +
                "\t\t\"coord\": [\n" +
                "\t\t\t-73.98513559999999,\n" +
                "\t\t\t40.7676919\n" +
                "\t\t],\n" +
                "\t\t\"street\": \"West   57 Street\",\n" +
                "\t\t\"zipcode\": \"10019\"\n" +
                "\t},\n" +
                "\t\"borough\": \"Manhattan\",\n" +
                "\t\"cuisine\": \"Irish\",\n" +
                "\t\"grades\": [\n" +
                "\t\t{\n" +
                "\t\t\t\"date\": {\n" +
                "\t\t\t\t\"$date\": \"2014-09-06T00:00:00Z\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\": \"A\",\n" +
                "\t\t\t\"score\": 2\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"date\": {\n" +
                "\t\t\t\t\"$date\": \"2013-07-22T00:00:00Z\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\": \"A\",\n" +
                "\t\t\t\"score\": 11\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"date\": {\n" +
                "\t\t\t\t\"$date\": \"2012-07-31T00:00:00Z\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\": \"A\",\n" +
                "\t\t\t\"score\": 12\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\t\"date\": {\n" +
                "\t\t\t\t\"$date\": \"2011-12-29T00:00:00Z\"\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\": \"A\",\n" +
                "\t\t\t\"score\": 12\n" +
                "\t\t}\n" +
                "\t],\n" +
                "\t\"name\": \"Dj Reynolds Pub And Restaurant\",\n" +
                "\t\"restaurant_id\": \"30191841\"\n" +
                "}", firstDocument.toJson(jsonWriterSettings),false);
    }

    @Test
    public void notLikeQuery() throws ParseException, JSONException {
        QueryConverter queryConverter1 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where address.street IS NOT NULL").build();
        QueryConverter queryConverter2 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where address.street LIKE '%Street'").build();
        QueryConverter queryConverter3 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where address.street NOT LIKE '%Street'").build();
        int count1 = Lists.newArrayList((Iterator) queryConverter1.run(mongoDatabase)).size();
        int count2 = Lists.newArrayList((Iterator) queryConverter2.run(mongoDatabase)).size();
        int count3 = Lists.newArrayList((Iterator) queryConverter3.run(mongoDatabase)).size();
        assertEquals(25359, count1);
        assertEquals(7499, count2);
        assertEquals(count1 - count2, count3);
    }

    @Test
    public void notRegexQuery() throws ParseException, JSONException {
        QueryConverter queryConverter1 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where address.street IS NOT NULL").build();
        QueryConverter queryConverter2 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where regexMatch(address.street,'.+Street$')").build();
        QueryConverter queryConverter3 = new QueryConverter.Builder().sqlString("select * from "+COLLECTION+" where notRegexMatch(address.street,'.+Street$')").build();
        int count1 = Lists.newArrayList((Iterator) queryConverter1.run(mongoDatabase)).size();
        int count2 = Lists.newArrayList((Iterator) queryConverter2.run(mongoDatabase)).size();
        int count3 = Lists.newArrayList((Iterator) queryConverter3.run(mongoDatabase)).size();
        assertEquals(25359, count1);
        assertEquals(7499, count2);
        assertEquals(count1 - count2, count3);
    }

    @Test
    public void objectIdQuery() throws ParseException, JSONException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select _id from " + COLLECTION
                + " where ObjectId('_id') = '54651022bffebc03098b4567'").build();
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            List<Document> documents = Lists.newArrayList(findIterable);
            assertEquals(1, documents.size());
            JSONAssert.assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4567\"\n"
                + "\t}\n" + "}", documents.get(0).toJson(jsonWriterSettings),false);
        } finally {
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")));
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")));
        }
    }

    @Test
    public void objectIdInQuery() throws ParseException, JSONException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select _id from " + COLLECTION
                + " where ObjectId('_id') IN ('54651022bffebc03098b4567','54651022bffebc03098b4568')").build();
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            List<Document> documents = Lists.newArrayList(findIterable);
            assertEquals(2, documents.size());
            JSONAssert.assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4567\"\n"
                + "\t}\n" + "}", documents.get(0).toJson(jsonWriterSettings),false);
            JSONAssert.assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4568\"\n"
                + "\t}\n" + "}", documents.get(1).toJson(jsonWriterSettings),false);
        } finally {
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")));
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")));
        }
    }

    @Test
    public void likeQueryWithProjection() throws ParseException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select address.building, address.coord from "+COLLECTION+" where address.street LIKE '%Street'").build();
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        JSONAssert.assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919]\n" +
                "\t}\n" +
                "}",documents.get(0).toJson(jsonWriterSettings),false);
    }

    @Test
    public void distinctQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select distinct borough from "+COLLECTION+" where address.street LIKE '%Street'").build();
        QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
        List<String> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        Collections.sort(results);
        assertEquals(Arrays.asList("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"),results);
    }
    
    @Test
    public void selectQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, cuisine from "+COLLECTION+" limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Bakery\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Brooklyn\",\n" + 
        		"	\"cuisine\" : \"Hamburgers\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Manhattan\",\n" + 
        		"	\"cuisine\" : \"Irish\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Brooklyn\",\n" + 
        		"	\"cuisine\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Queens\",\n" + 
        		"	\"cuisine\" : \"Jewish/Kosher\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Queens\",\n" + 
        		"	\"cuisine\" : \"American \"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectQueryAlias() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c from "+COLLECTION+" limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"Bakery\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"c\" : \"Hamburgers\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"c\" : \"Irish\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"c\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"c\" : \"Jewish/Kosher\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"c\" : \"American \"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectOrderByQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, cuisine from "+COLLECTION+" order by borough asc,cuisine desc limit 10").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(10, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Thai\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Thai\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectOrderByQueryOffset() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, cuisine from "+COLLECTION+" order by borough asc,cuisine desc limit 5 offset 5").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"},{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Tex-Mex\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectOrderByAliasQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c from "+COLLECTION+" order by borough asc,cuisine asc limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectOrderByAliasOneInAliasQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c from "+COLLECTION+" order by b asc,cuisine asc limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"}]",toJson(results),false);
    }

    
    @Test
    public void selectOrderByAliasBothInAliasQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c from "+COLLECTION+" order by borough asc,c asc limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void selectOrderByAliasTwoInAliasQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c from "+COLLECTION+" order by b asc,c asc limit 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"},{\n" + 
        		"	\"b\" : \"Bronx\",\n" + 
        		"	\"c\" : \"African\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countGroupByQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, count(borough) from "+COLLECTION+" GROUP BY borough").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" +
        		"	\"count\" : 51,\n" + 
        		"	\"borough\" : \"Missing\"\n" + 
        		"},{\n" + 
        		"	\"count\" : 969,\n" + 
        		"	\"borough\" : \"Staten Island\"\n" + 
        		"},{\n" + 
        		"	\"count\" : 10259,\n" + 
        		"	\"borough\" : \"Manhattan\"\n" + 
        		"},{\n" + 
        		"	\"count\" : 6086,\n" + 
        		"	\"borough\" : \"Brooklyn\"\n" + 
        		"},{\n" + 
        		"	\"count\" : 5656,\n" + 
        		"	\"borough\" : \"Queens\"\n" + 
        		"},{\n" + 
        		"	\"count\" : 2338,\n" + 
        		"	\"borough\" : \"Bronx\"\n" + 
        		"}]",toJson(results), false);
    }

    @Test
    public void countGroupBySortByCountQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, count(borough) from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" +
                "\t\"count\" : 10259,\n" +
                "\t\"borough\" : \"Manhattan\"\n" +
                "},{\n" +
                "\t\"count\" : 6086,\n" +
                "\t\"borough\" : \"Brooklyn\"\n" +
                "},{\n" +
                "\t\"count\" : 5656,\n" +
                "\t\"borough\" : \"Queens\"\n" +
                "},{\n" +
                "\t\"count\" : 2338,\n" +
                "\t\"borough\" : \"Bronx\"\n" +
                "},{\n" +
                "\t\"count\" : 969,\n" +
                "\t\"borough\" : \"Staten Island\"\n" +
                "},{\n" +
                "\t\"count\" : 51,\n" +
                "\t\"borough\" : \"Missing\"\n" +
                "}]",toJson(results),false);
    }
    
    @Test
    public void countGroupBySortByCountMixedQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, count(borough) as co from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" +
                "\t\"co\" : 10259,\n" +
                "\t\"borough\" : \"Manhattan\"\n" +
                "},{\n" +
                "\t\"co\" : 6086,\n" +
                "\t\"borough\" : \"Brooklyn\"\n" +
                "},{\n" +
                "\t\"co\" : 5656,\n" +
                "\t\"borough\" : \"Queens\"\n" +
                "},{\n" +
                "\t\"co\" : 2338,\n" +
                "\t\"borough\" : \"Bronx\"\n" +
                "},{\n" +
                "\t\"co\" : 969,\n" +
                "\t\"borough\" : \"Staten Island\"\n" +
                "},{\n" +
                "\t\"co\" : 51,\n" +
                "\t\"borough\" : \"Missing\"\n" +
                "}]",toJson(results),false);
    }
    
    @Test
    public void countGroupBySortByCountAliasMixedQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, count(borough) as co from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY co DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" +
                "\t\"co\" : 10259,\n" +
                "\t\"borough\" : \"Manhattan\"\n" +
                "},{\n" +
                "\t\"co\" : 6086,\n" +
                "\t\"borough\" : \"Brooklyn\"\n" +
                "},{\n" +
                "\t\"co\" : 5656,\n" +
                "\t\"borough\" : \"Queens\"\n" +
                "},{\n" +
                "\t\"co\" : 2338,\n" +
                "\t\"borough\" : \"Bronx\"\n" +
                "},{\n" +
                "\t\"co\" : 969,\n" +
                "\t\"borough\" : \"Staten Island\"\n" +
                "},{\n" +
                "\t\"co\" : 51,\n" +
                "\t\"borough\" : \"Missing\"\n" +
                "}]",toJson(results),false);
    }
    
    @Test
    public void countGroupBySortByCountAliasAllQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough b, count(borough) as co from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" +
                "\t\"co\" : 10259,\n" +
                "\t\"b\" : \"Manhattan\"\n" +
                "},{\n" +
                "\t\"co\" : 6086,\n" +
                "\t\"b\" : \"Brooklyn\"\n" +
                "},{\n" +
                "\t\"co\" : 5656,\n" +
                "\t\"b\" : \"Queens\"\n" +
                "},{\n" +
                "\t\"co\" : 2338,\n" +
                "\t\"b\" : \"Bronx\"\n" +
                "},{\n" +
                "\t\"co\" : 969,\n" +
                "\t\"b\" : \"Staten Island\"\n" +
                "},{\n" +
                "\t\"co\" : 51,\n" +
                "\t\"b\" : \"Missing\"\n" +
                "}]",toJson(results),false);
    }
    
    @Test
    public void countGroupByNestedFieldSortByCountAliasAllQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select address.zipcode as az, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode asc limit 6\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"az\" : \"\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"az\" : \"07005\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"az\" : \"10000\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 520,\n" + 
        		"	\"az\" : \"10001\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 471,\n" + 
        		"	\"az\" : \"10002\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 686,\n" + 
        		"	\"az\" : \"10003\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countGroupByNestedFieldSortByCountAliasAllQueryOffset() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select address.zipcode as az, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode asc limit 3 offset 3\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(3, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"co\" : 520,\n" + 
        		"	\"az\" : \"10001\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 471,\n" + 
        		"	\"az\" : \"10002\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 686,\n" + 
        		"	\"az\" : \"10003\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countGroupByNestedFieldSortByCountQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select address.zipcode, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode limit 6\n" +
                "ORDER BY count(borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"07005\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10000\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 520,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10001\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 471,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10002\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 686,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10003\"\n" + 
        		"	}\n" + 
        		"}]",toJson(results),false);
    }

    @Test
    public void countGroupByQueryLimit() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder()
                .sqlString("select borough, count(borough) from "+COLLECTION+" GROUP BY borough order by borough asc LIMIT 2").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        JSONAssert.assertEquals(toJson(Arrays.asList(new Document("count",2338).append("borough","Bronx"),
                new Document("count",6086).append("borough","Brooklyn")
        )),toJson(results),false);
    }
    
    @Test
    public void countGroupByQueryLimitOffset() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, count(borough) from "+COLLECTION+" GROUP BY borough order by borough asc LIMIT 1 OFFSET 1").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals(toJson(Arrays.asList(new Document("count",6086).append("borough","Brooklyn")
        )),toJson(results),false);
    }

    @Test
    public void countGroupByQueryMultipleColumns() throws ParseException, IOException,
        JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough, cuisine, count(*) from "+COLLECTION+" GROUP BY borough, cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("count") > 500;
            }

            @Override
            public boolean test(final Document input) {
                return apply(input);
            }
        }));

        JSONAssert.assertEquals("[{\n" +
            "	\"count\" : 680,\n" +
            "	\"borough\" : \"Manhattan\",\n" +
            "	\"cuisine\" : \"Café/Coffee/Tea\"\n" +
            "},{\n" +
            "	\"count\" : 510,\n" +
            "	\"borough\" : \"Manhattan\",\n" +
            "	\"cuisine\" : \"Chinese\"\n" +
            "},{\n" +
            "	\"count\" : 728,\n" +
            "	\"borough\" : \"Queens\",\n" +
            "	\"cuisine\" : \"Chinese\"\n" +
            "},{\n" +
            "	\"count\" : 1040,\n" +
            "	\"borough\" : \"Queens\",\n" +
            "	\"cuisine\" : \"American \"\n" +
            "},{\n" +
            "	\"count\" : 3205,\n" +
            "	\"borough\" : \"Manhattan\",\n" +
            "	\"cuisine\" : \"American \"\n" +
            "},{\n" +
            "	\"count\" : 1273,\n" +
            "	\"borough\" : \"Brooklyn\",\n" +
            "	\"cuisine\" : \"American \"\n" +
            "},{\n" +
            "	\"count\" : 763,\n" +
            "	\"borough\" : \"Brooklyn\",\n" +
            "	\"cuisine\" : \"Chinese\"\n" +
            "},{\n" +
            "	\"count\" : 621,\n" +
            "	\"borough\" : \"Manhattan\",\n" +
            "	\"cuisine\" : \"Italian\"\n" +
            "}]", toJson(filteredResults), false);
    }
    
    @Test
    public void countGroupByQueryMultipleColumnsAliasMixed()
        throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine, count(*) as co from "+COLLECTION+" GROUP BY borough, cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("co") > 500;
            }

            @Override
            public boolean test(final Document input) {
                return apply(input);
            }
        }));

        JSONAssert.assertEquals("[{\n" +
        		"	\"co\" : 680,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"cuisine\" : \"Café/Coffee/Tea\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 510,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"cuisine\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 728,\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"cuisine\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1040,\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"cuisine\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 3205,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"cuisine\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1273,\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"cuisine\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 763,\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"cuisine\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 621,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"cuisine\" : \"Italian\"\n" + 
        		"}]",toJson(filteredResults), false);
    }
    
    @Test
    public void countGroupByQueryMultipleColumnsAliasAll()
        throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select borough as b, cuisine as c, count(*) as co from "+COLLECTION+" GROUP BY borough, cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("co") > 500;
            }

            @Override
            public boolean test(final Document input) {
                return apply(input);
            }
        }));

        JSONAssert.assertEquals("[{\n" +
        		"	\"co\" : 680,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"c\" : \"Café/Coffee/Tea\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 510,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"c\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 728,\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"c\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1040,\n" + 
        		"	\"b\" : \"Queens\",\n" + 
        		"	\"c\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 3205,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"c\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 1273,\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"c\" : \"American \"\n" + 
        		"},{\n" + 
        		"	\"co\" : 763,\n" + 
        		"	\"b\" : \"Brooklyn\",\n" + 
        		"	\"c\" : \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"co\" : 621,\n" + 
        		"	\"b\" : \"Manhattan\",\n" + 
        		"	\"c\" : \"Italian\"\n" + 
        		"}]",toJson(filteredResults), false);
    }
    
    @Test
    public void simpleTableAlias() throws ParseException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.address.building, c.address.coord from "+COLLECTION+" as c where c.address.street LIKE '%Street'").build();
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        JSONAssert.assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919]\n" +
                "\t}\n" +
                "}",documents.get(0).toJson(jsonWriterSettings),false);
    }
    
    @Test
    public void simpleTableAliasGroup() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.address.zipcode, count(c.borough) as co from "+COLLECTION+" as c GROUP BY c.address.zipcode order by c.address.zipcode limit 6\n" +
                "ORDER BY count(c.borough) DESC;").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"07005\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 1,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10000\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 520,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10001\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 471,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10002\"\n" + 
        		"	}\n" + 
        		"},{\n" + 
        		"	\"co\" : 686,\n" + 
        		"	\"address\" : {\n" + 
        		"		\"zipcode\" : \"10003\"\n" + 
        		"	}\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubquery() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from(select borough, cuisine from "+COLLECTION+" limit 1)").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Bakery\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAlias() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select * from(select borough, cuisine from "+COLLECTION+" order by restaurant_id asc limit 1) as c").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\",\n" + 
        		"	\"cuisine\" : \"Bakery\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAlias_Project() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.borough from(select borough, cuisine from "+COLLECTION+" order by restaurant_id asc limit 1) as c").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\"\n" +  
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAlias_ProjectLimit() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.borough from(select borough, cuisine from "+COLLECTION+" order by restaurant_id asc limit 2) as c limit 1").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"borough\" : \"Bronx\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAlias_WhereProject() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.restaurant_id from(select borough, cuisine, restaurant_id from "+COLLECTION+" order by restaurant_id asc limit 6) as c where c.cuisine = 'Hamburgers'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"restaurant_id\" : \"30112340\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroup_WhereProject() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, c.c as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" group by borough, cuisine) as c where c.cuisine = 'Hamburgers' and c.borough ='Manhattan'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"cuisine\" : \"Hamburgers\",\n" +
        		"	\"c\" : 124\n" +
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroupWhere_WhereProject() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, c.c as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" where cuisine = 'Hamburgers' group by borough, cuisine) as c where c.borough ='Manhattan'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"cuisine\" : \"Hamburgers\",\n" +
        		"	\"c\" : 124\n" +
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroup_WhereProjectGroup() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" group by borough, cuisine) as c where c.cuisine = 'Italian' group by cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"cuisine\" : \"Italian\",\n" +
        		"	\"c\" : 1069\n" +
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroupSort_WhereProjectGroup() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" group by borough, cuisine order by count(*) asc, borough, cuisine limit 300) as c where c.c > 100 group by cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(3, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"c\": 104,\n" + 
        		"	\"cuisine\": \"Asian\"\n" + 
        		"},{\n" + 
        		"	\"c\": 102,\n" + 
        		"	\"cuisine\": \"Pizza/Italian\"\n" + 
        		"},{\n" + 
        		"	\"c\": 102,\n" + 
        		"	\"cuisine\": \"Hamburgers\"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroup_WhereProjectGroupSort() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" group by borough, cuisine) as c where c.c > 500 group by c.cuisine order by cuisine desc ").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(4, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"c\": 621,\n" + 
        		"	\"cuisine\": \"Italian\"\n" + 
        		"},{\n" + 
        		"	\"c\": 2001,\n" + 
        		"	\"cuisine\": \"Chinese\"\n" + 
        		"},{\n" + 
        		"	\"c\": 680,\n" + 
        		"	\"cuisine\": \"Café/Coffee/Tea\"\n" + 
        		"},{\n" + 
        		"	\"c\": 5518,\n" + 
        		"	\"cuisine\": \"American \"\n" + 
        		"}]",toJson(results),false);
    }
    
    @Test
    public void simpleSubqueryAliasGroupSort_WhereProjectGroupSort() throws ParseException, JSONException, IOException {
    	QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select c.cuisine, sum(c.c) as c  from(select borough, cuisine, count(*) as c from "+COLLECTION+" group by borough, cuisine order by count(*) desc, borough asc, cuisine desc limit 3) as c where c.c > 1000 group by cuisine order by cuisine asc").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"cuisine\" : \"American \",\n" +
        		"	\"c\" : 5518\n" +
        		"}]",toJson(results),false);
    }


    @Test
    public void avgQueryAliasWhereHaving() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select avg(_id) as av from "+COLLECTION_FILMS+" having (av > 10)").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" +
                "	\"av\": 500.5\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void countGroupByQueryHavingByCountAlias() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select cuisine, count(cuisine) as cc from "+COLLECTION+" WHERE borough = 'Manhattan' GROUP BY cuisine HAVING cc > 500").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        queryConverter.write(System.out);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(4, results.size());
        JSONAssert.assertEquals("[{\n" + 
        		"	\"cuisine\": \"Chinese\",\n" + 
        		"	\"cc\": 510\n" +  
        		"}," +
        		"{\n" + 
        		"	\"cuisine\": \"Italian\",\n" + 
        		"	\"cc\": 621\n" +  
        		"}," +
        		"{\n" + 
        		"	\"cuisine\": \"Café/Coffee/Tea\",\n" + 
        		"	\"cc\": 680\n" +
        		"}," +
        		"{\n" + 
        		"	\"cuisine\": \"American \",\n" + 
        		"	\"cc\": 3205\n" +
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) from "+COLLECTION+" where address.street LIKE '%Street'").build();
        long count  = queryConverter.run(mongoDatabase);
        assertEquals(7499, count);
    }
    
    @Test
    public void countQueryAlias() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) as c from "+COLLECTION).build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"c\": " + TOTAL_TEST_RECORDS_PRIMER + "\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) as c from "+COLLECTION+" where address.street LIKE '%Street'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"c\": 7499\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void sumQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select sum(_id) as s from "+COLLECTION_FILMS+" where Category = 'Sports'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"s\": 39584\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void minQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select min(_id) as mi from "+COLLECTION_FILMS+" where Category = 'Sports'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"mi\": 10\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void maxQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select max(_id) as ma from "+COLLECTION_FILMS+" where Category = 'Sports'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"ma\": 940\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void avgQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select avg(_id) as av from "+COLLECTION_FILMS+" where Category = 'Sports'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"av\": 534.918918918919\n" +    
        		"}]",toJson(results),false);
    }
    
    @Test
    public void allGroupsQueryAliasWhere() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) as c, sum(_id) as s, min(_id) as mi, max(_id) as ma, avg(_id) as av from "+COLLECTION_FILMS+" where Category = 'Sports'").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" +  
        		"	\"s\": 39584,\n" +
        		"	\"mi\": 10,\n" +
        		"	\"ma\": 940,\n" +
        		"	\"av\": 534.918918918919,\n" +
        		"	\"c\": 74\n" +  
        		"}]",toJson(results),false);
    }
    
    @Test
    public void countByBoroughSubquery() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select count(*) from(select borough from " + COLLECTION + " group by borough)").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        JSONAssert.assertEquals("[{\n" + 
        		"	\"count\": 6\n" +    
        		"}]",toJson(results),false);
    }

    @Test
    public void deleteQuery() throws ParseException {
        String collection = "new_collection";
        MongoCollection newCollection = mongoDatabase.getCollection(collection);
        try {
            newCollection.insertOne(new Document("_id", "1").append("key", "value"));
            newCollection.insertOne(new Document("_id", "2").append("key", "value"));
            newCollection.insertOne(new Document("_id", "3").append("key", "value"));
            newCollection.insertOne(new Document("_id", "4").append("key2", "value2"));
            assertEquals(3, newCollection.count(new BsonDocument("key", new BsonString("value"))));
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString("delete from " + collection + " where key = 'value'").build();
            long deleteCount = queryConverter.run(mongoDatabase);
            assertEquals(3, deleteCount);
            assertEquals(1, newCollection.countDocuments());
        } finally {
            newCollection.drop();
        }
    }



    protected static String toJson(List<Document> documents) throws IOException {
        StringWriter stringWriter = new StringWriter();
        IOUtils.write("[", stringWriter);
        IOUtils.write(Joiner.on(",").join(Lists.transform(documents, new com.google.common.base.Function<Document, String>() {
            @Override
            public String apply(Document document) {
                return document.toJson(jsonWriterSettings);
            }
        })),stringWriter);
        IOUtils.write("]", stringWriter);
        return stringWriter.toString();
    }
}
