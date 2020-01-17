package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.distribution.GenericVersion;

import org.apache.commons.io.IOUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.*;
import java.net.ServerSocket;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class QueryConverterIT {

    private static final int TOTAL_TEST_RECORDS = 25359;
    private static MongodStarter starter = MongodStarter.getDefaultInstance();
    private static MongodProcess mongodProcess;
    private static MongodExecutable mongodExecutable;
    private static int port = getRandomFreePort();
    private static MongoClient mongoClient;
    private static final String DATABASE = "local";
    private static final String COLLECTION = "my_collection";
    private static MongoDatabase mongoDatabase;
    private static MongoCollection mongoCollection;
    private static JsonWriterSettings jsonWriterSettings = new JsonWriterSettings(JsonMode.STRICT, "\t", "\n");

    @BeforeClass
    public static void beforeClass() throws IOException {
        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Versions.withFeatures(new GenericVersion("2012plus-4.2.2"),Feature.SYNC_DELAY,Feature.NO_HTTP_INTERFACE_ARG))
                .cmdOptions(new MongoCmdOptionsBuilder()
            		.useNoPrealloc(false)
            		.useSmallFiles(false)
            		.build())
                .net(new Net("localhost",port, false))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();
        mongoClient = new MongoClient("localhost",port);


        mongoDatabase = mongoClient.getDatabase(DATABASE);
        mongoCollection = mongoDatabase.getCollection(COLLECTION);

        List<Document> documents = new ArrayList<>(TOTAL_TEST_RECORDS);
        try(InputStream inputStream = QueryConverterIT.class.getResourceAsStream("/primer-dataset.json");
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
            mongoCollection.bulkWrite(iterator.next());
        }

        assertEquals(TOTAL_TEST_RECORDS,mongoCollection.count());

    }

    @AfterClass
    public static void afterClass() {
        mongoClient.close();
        mongodProcess.stop();
        mongodExecutable.stop();
    }

    @Test
    public void likeQuery() throws ParseException, JSONException {
        QueryConverter queryConverter = new QueryConverter("select * from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        Document firstDocument = documents.get(0);
        firstDocument.remove("_id");
        JSONAssert.assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919],\n" +
                "\t\t\"street\" : \"West   57 Street\",\n" +
                "\t\t\"zipcode\" : \"10019\"\n" +
                "\t},\n" +
                "\t\"borough\" : \"Manhattan\",\n" +
                "\t\"cuisine\" : \"Irish\",\n" +
                "\t\"grades\" : [{\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1409961600000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 2\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1374451200000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 11\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1343692800000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 12\n" +
                "\t\t}, {\n" +
                "\t\t\t\"date\" : {\n" +
                "\t\t\t\t\"$date\" : 1325116800000\n" +
                "\t\t\t},\n" +
                "\t\t\t\"grade\" : \"A\",\n" +
                "\t\t\t\"score\" : 12\n" +
                "\t\t}],\n" +
                "\t\"name\" : \"Dj Reynolds Pub And Restaurant\",\n" +
                "\t\"restaurant_id\" : \"30191841\"\n" +
                "}", firstDocument.toJson(jsonWriterSettings),false);
    }

    @Test
    public void objectIdQuery() throws ParseException, JSONException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter("select _id from " + COLLECTION
                + " where ObjectId('_id') = '54651022bffebc03098b4567'");
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
            QueryConverter queryConverter = new QueryConverter("select _id from " + COLLECTION
                + " where ObjectId('_id') IN ('54651022bffebc03098b4567','54651022bffebc03098b4568')");
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
        QueryConverter queryConverter = new QueryConverter("select address.building, address.coord from "+COLLECTION+" where address.street LIKE '%Street'");
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
        QueryConverter queryConverter = new QueryConverter("select distinct borough from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
        List<String> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        Collections.sort(results);
        assertEquals(Arrays.asList("Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"),results);
    }
    
    @Test
    public void selectQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter("select borough, cuisine from "+COLLECTION+" limit 6");
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
        QueryConverter queryConverter = new QueryConverter("select borough as b, cuisine as c from "+COLLECTION+" limit 6");
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
        QueryConverter queryConverter = new QueryConverter("select borough, cuisine from "+COLLECTION+" order by borough asc,cuisine desc limit 10");
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
        QueryConverter queryConverter = new QueryConverter("select borough, cuisine from "+COLLECTION+" order by borough asc,cuisine desc limit 5 offset 5");
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
        QueryConverter queryConverter = new QueryConverter("select borough as b, cuisine as c from "+COLLECTION+" order by borough asc,cuisine asc limit 6");
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
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough");
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
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;");
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
    public void countGroupBySortByCountAliasMixedQuery() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) as co from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;");
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
        QueryConverter queryConverter = new QueryConverter("select borough b, count(borough) as co from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;");
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
        QueryConverter queryConverter = new QueryConverter("select address.zipcode as az, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode asc limit 6\n" +
                "ORDER BY count(borough) DESC;");
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
        QueryConverter queryConverter = new QueryConverter("select address.zipcode as az, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode asc limit 3 offset 3\n" +
                "ORDER BY count(borough) DESC;");
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
        QueryConverter queryConverter = new QueryConverter("select address.zipcode, count(borough) as co from "+COLLECTION+" GROUP BY address.zipcode order by address.zipcode limit 6\n" +
                "ORDER BY count(borough) DESC;");
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
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough order by borough asc LIMIT 2");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        JSONAssert.assertEquals(toJson(Arrays.asList(new Document("count",2338).append("borough","Bronx"),
                new Document("count",6086).append("borough","Brooklyn")
        )),toJson(results),false);
    }
    
    @Test
    public void countGroupByQueryLimitOffset() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough order by borough asc LIMIT 1 OFFSET 1");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals(toJson(Arrays.asList(new Document("count",6086).append("borough","Brooklyn")
        )),toJson(results),false);
    }

    @Test
    public void countGroupByQueryMultipleColumns() throws ParseException, IOException,
        JSONException {
        QueryConverter queryConverter = new QueryConverter("select borough, cuisine, count(*) from "+COLLECTION+" GROUP BY borough, cuisine");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("count") > 500;
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
        QueryConverter queryConverter = new QueryConverter("select borough as b, cuisine, count(*) as co from "+COLLECTION+" GROUP BY borough, cuisine");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("co") > 500;
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
        QueryConverter queryConverter = new QueryConverter("select borough as b, cuisine as c, count(*) as co from "+COLLECTION+" GROUP BY borough, cuisine");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(365, results.size());

        List<Document> filteredResults = Lists.newArrayList(Collections2.filter(results, new Predicate<Document>() {
            @Override
            public boolean apply(Document document) {
                return document.getInteger("co") > 500;
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
    public void countQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select count(*) from "+COLLECTION+" where address.street LIKE '%Street'");
        long count  = queryConverter.run(mongoDatabase);
        assertEquals(7499, count);
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
            QueryConverter queryConverter = new QueryConverter("delete from " + collection + " where key = 'value'");
            long deleteCount = queryConverter.run(mongoDatabase);
            assertEquals(3, deleteCount);
            assertEquals(1, newCollection.count());
        } finally {
            newCollection.drop();
        }
    }

    private static int getRandomFreePort() {
        Random r = new Random();
        int count = 0;

        while (count < 13) {
            int port = r.nextInt((1 << 16) - 1024) + 1024;

            ServerSocket so = null;
            try {
                so = new ServerSocket(port);
                so.setReuseAddress(true);
                return port;
            } catch (IOException ioe) {

            } finally {
                if (so != null)
                    try {
                        so.close();
                    } catch (IOException e) {}
            }

        }

        throw new RuntimeException("Unable to find port");
    }

    private static String toJson(List<Document> documents) throws IOException {
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
