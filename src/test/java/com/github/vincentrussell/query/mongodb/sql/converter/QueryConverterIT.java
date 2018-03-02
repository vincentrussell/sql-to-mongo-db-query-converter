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
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.apache.commons.io.IOUtils;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
                .version(Version.Main.PRODUCTION)
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
    public void likeQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select * from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        Document firstDocument = documents.get(0);
        firstDocument.remove("_id");
        assertEquals("{\n" +
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
                "}", firstDocument.toJson(jsonWriterSettings));
    }

    @Test
    public void likeQueryWithProjection() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select address.building, address.coord from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
        List<Document> documents = Lists.newArrayList(findIterable);
        assertEquals(7499, documents.size());
        assertEquals("{\n" +
                "\t\"address\" : {\n" +
                "\t\t\"building\" : \"351\",\n" +
                "\t\t\"coord\" : [-73.98513559999999, 40.7676919]\n" +
                "\t}\n" +
                "}",documents.get(0).toJson(jsonWriterSettings));
    }

    @Test
    public void distinctQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select distinct borough from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
        List<String> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        assertEquals(Arrays.asList("Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"),results);
    }

    @Test
    public void countGroupByQuery() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        assertEquals("[{\n" +
                "\t\"_id\" : \"Missing\",\n" +
                "\t\"count\" : 51\n" +
                "},{\n" +
                "\t\"_id\" : \"Staten Island\",\n" +
                "\t\"count\" : 969\n" +
                "},{\n" +
                "\t\"_id\" : \"Manhattan\",\n" +
                "\t\"count\" : 10259\n" +
                "},{\n" +
                "\t\"_id\" : \"Bronx\",\n" +
                "\t\"count\" : 2338\n" +
                "},{\n" +
                "\t\"_id\" : \"Queens\",\n" +
                "\t\"count\" : 5656\n" +
                "},{\n" +
                "\t\"_id\" : \"Brooklyn\",\n" +
                "\t\"count\" : 6086\n" +
                "}]",toJson(results));
    }

    @Test
    public void countGroupByQuerySortByCount() throws ParseException, IOException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough\n" +
                "ORDER BY count(borough) DESC;");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(6, results.size());
        assertEquals("[{\n" +
                "\t\"_id\" : \"Manhattan\",\n" +
                "\t\"count\" : 10259\n" +
                "},{\n" +
                "\t\"_id\" : \"Brooklyn\",\n" +
                "\t\"count\" : 6086\n" +
                "},{\n" +
                "\t\"_id\" : \"Queens\",\n" +
                "\t\"count\" : 5656\n" +
                "},{\n" +
                "\t\"_id\" : \"Bronx\",\n" +
                "\t\"count\" : 2338\n" +
                "},{\n" +
                "\t\"_id\" : \"Staten Island\",\n" +
                "\t\"count\" : 969\n" +
                "},{\n" +
                "\t\"_id\" : \"Missing\",\n" +
                "\t\"count\" : 51\n" +
                "}]",toJson(results));
    }

    @Test
    public void countGroupByQueryLimit() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select borough, count(borough) from "+COLLECTION+" GROUP BY borough LIMIT 2");
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        assertEquals(Arrays.asList(new Document("_id","Missing").append("count",51),
                new Document("_id","Staten Island").append("count",969)
        ),results);
    }

    @Test
    public void countGroupByQueryMultipleColumns() throws ParseException, IOException {
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

        assertEquals("[{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 510\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Queens\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 1040\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Café/Coffee/Tea\"\n" +
                "\t},\n" +
                "\t\"count\" : 680\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"Italian\"\n" +
                "\t},\n" +
                "\t\"count\" : 621\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Brooklyn\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 1273\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Manhattan\",\n" +
                "\t\t\"cuisine\" : \"American \"\n" +
                "\t},\n" +
                "\t\"count\" : 3205\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Queens\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 728\n" +
                "},{\n" +
                "\t\"_id\" : {\n" +
                "\t\t\"borough\" : \"Brooklyn\",\n" +
                "\t\t\"cuisine\" : \"Chinese\"\n" +
                "\t},\n" +
                "\t\"count\" : 763\n" +
                "}]",toJson(filteredResults));
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
