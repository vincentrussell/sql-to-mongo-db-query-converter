package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class QueryConverterIT {

    public static final int TOTAL_TEST_RECORDS = 25359;
    static MongodStarter starter = MongodStarter.getDefaultInstance();
    static MongodProcess mongodProcess;
    static MongodExecutable mongodExecutable;
    static int port = getRandomFreePort();
    static MongoClient mongoClient;
    static final String DATABASE = "local";
    static final String COLLECTION = "my_collection";
    static MongoDatabase mongoDatabase;
    static MongoCollection mongoCollection;
    static JsonWriterSettings jsonWriterSettings = new JsonWriterSettings(JsonMode.STRICT, "\t", "\n");

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
        QueryConverter queryConverter = new QueryConverter("select distinct address.building from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<String> distinctIterable = queryConverter.run(mongoDatabase);
        assertEquals(1957, Iterators.size(distinctIterable));
    }

    @Test
    public void countQuery() throws ParseException {
        QueryConverter queryConverter = new QueryConverter("select count(*) from "+COLLECTION+" where address.street LIKE '%Street'");
        QueryResultIterator<Long> findIterable = queryConverter.run(mongoDatabase);
        assertEquals(Long.valueOf(7499), Iterators.get(findIterable,0));
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
}
