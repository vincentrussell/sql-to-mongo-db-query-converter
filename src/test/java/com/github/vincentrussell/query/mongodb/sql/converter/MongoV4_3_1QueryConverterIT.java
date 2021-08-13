package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.rule.MongoRule;
import com.google.common.collect.Lists;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.junit.ClassRule;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoV4_3_1QueryConverterIT extends AbstractQueryConverterIT {

    @ClassRule
    public static MongoRule mongoRule = new MongoRule(Version.V4_3_1);

    public MongoRule getMongoRule() {
        return mongoRule;
    }

    @Test
    public void countGroupByQueryHavingByCount() throws ParseException, IOException, JSONException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select cuisine, count(cuisine) from "+COLLECTION+" WHERE borough = 'Manhattan' GROUP BY cuisine HAVING count(cuisine) > 500").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        queryConverter.write(System.out);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(4, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Chinese\",\n" +
                "	\"count\": 510\n" +
                "}," +
                "{\n" +
                "	\"cuisine\": \"Italian\",\n" +
                "	\"count\": 621\n" +
                "}," +
                "{\n" +
                "	\"cuisine\": \"Café/Coffee/Tea\",\n" +
                "	\"count\": 680\n" +
                "}," +
                "{\n" +
                "	\"cuisine\": \"American \",\n" +
                "	\"count\": 3205\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void havingAliasMultiCond() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select cuisine as cuisine, count(*) as c from "+COLLECTION+" where 1=1 group by Restaurant.cuisine having c >= 5 and c <= 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Nuts/Confectionary\",\n" +
                "	\"c\": 6\n" +
                "}," +
                "{\n" +
                "	\"cuisine\": \"Czech\",\n" +
                "	\"c\": 6\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void havingMultiCond() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select cuisine, count(*) as c from "+COLLECTION+" where 1=1 group by Restaurant.cuisine having count(*) >= 5 and count(*) <= 6").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(2, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Nuts/Confectionary\",\n" +
                "	\"c\": 6\n" +
                "}," +
                "{\n" +
                "	\"cuisine\": \"Czech\",\n" +
                "	\"c\": 6\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void joinInSubqueryAndJoinAgain() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select t.cuisine as cuisine, max(t.total) as maxi, count(*) as coxi from "+COLLECTION+" as r inner join (select r.cuisine as cuisine, r.borough as borough, trest.totalrestaurats as total from "+COLLECTION+" as r inner join (select cuisine, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats desc, cuisine asc, borough limit 15) as t on r.cuisine = t.cuisine and r.borough = t.borough group by t.cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"maxi\": 6183,\n" +
                "	\"coxi\": 6165,\n" +
                "	\"cuisine\": \"American \"\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void joinInSubqueryByOne() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select t.cuisine, max(t.total) as maxi from (select r.cuisine as cuisine, trest.totalrestaurats as total from "+COLLECTION+" as r inner join (select cuisine, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats desc, cuisine asc limit 15) as t group by t.cuisine").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\" : \"American \",\n" +
                "	\"maxi\" : 6183\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void objectIdEqQueryFnInside() throws ParseException, JSONException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        try {
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select _id from " + COLLECTION
                    + " where _id = OID('54651022bffebc03098b4567')").build();
            QueryResultIterator<Document> findIterable = queryConverter.run(mongoDatabase);
            List<Document> documents = Lists.newArrayList(findIterable);
            assertEquals(1, documents.size());
            JSONAssert.assertEquals("{\n" + "\t\"_id\" : {\n" + "\t\t\"$oid\" : \"54651022bffebc03098b4567\"\n"
                    + "\t}\n" + "}", documents.get(0).toJson(jsonWriterSettings),false);
        } finally {
            mongoCollection.deleteOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")));
        }
    }

    @Test
    public void objectIdInQueryFnInside() throws ParseException, JSONException {
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4567")).append("key", "value1"));
        mongoCollection.insertOne(new Document("_id", new ObjectId("54651022bffebc03098b4568")).append("key", "value2"));
        try {
            QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select _id from " + COLLECTION
                    + " where _id IN (OID('54651022bffebc03098b4567'),OID('54651022bffebc03098b4568'))").build();
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
    public void simpleInnerJoin() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select t1.Phone as Phonet1, t2.managerStaffId as managerStaffIdt2 from "+COLLECTION_CUSTOMERS+" as t1 inner join " + COLLECTION_STORES + " as t2 on t1.Country = t2.Country").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(5, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"Phonet1\": \"247646995453\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "},{\n" +
                "	\"Phonet1\": \"615964523510\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "},{\n" +
                "	\"Phonet1\": \"145720452260\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "},{\n" +
                "	\"Phonet1\": \"164414772677\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "},{\n" +
                "	\"Phonet1\": \"856872225376\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void simpleInnerJoinByTwoFields() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select t1.Phone as Phonet1, t2.managerStaffId as managerStaffIdt2 from "+COLLECTION_CUSTOMERS+" as t1 inner join " + COLLECTION_STORES + " as t2 on t1.Country = t2.Country and t1.City = t2.City").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(1, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"Phonet1\": \"247646995453\",\n" +
                "	\"managerStaffIdt2\": \"1\"\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void subqueryJoinByOneGetMaxOfGroup() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select r.cuisine as cuisine, trest.totalrestaurats as total from "+COLLECTION+" as r inner join (select cuisine, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine) as trest on r.cuisine = trest.cuisine order by trest.totalrestaurats asc, cuisine asc limit 15").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(15, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Californian\",\n" +
                "	\"total\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"Chilean\",\n" +
                "	\"total\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"Creole/Cajun\",\n" +
                "	\"total\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"Polynesian\",\n" +
                "	\"total\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"CafÃ©/Coffee/Tea\",\n" +
                "	\"total\": 2\n" +
                "},{\n" +
                "	\"cuisine\": \"CafÃ©/Coffee/Tea\",\n" +
                "	\"total\": 2\n" +
                "},{\n" +
                "	\"cuisine\": \"Iranian\",\n" +
                "	\"total\": 2\n" +
                "},{\n" +
                "	\"cuisine\": \"Iranian\",\n" +
                "	\"total\": 2\n" +
                "},{\n" +
                "	\"cuisine\": \"Hawaiian\",\n" +
                "	\"total\": 3\n" +
                "},{\n" +
                "	\"cuisine\": \"Hawaiian\",\n" +
                "	\"total\": 3\n" +
                "},{\n" +
                "	\"cuisine\": \"Hawaiian\",\n" +
                "	\"total\": 3\n" +
                "},{\n" +
                "	\"cuisine\": \"Soups\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Soups\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Soups\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Soups\",\n" +
                "	\"total\": 4\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void subqueryJoinByTwoGetMaxOfGroup() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select r.cuisine as cuisine, r.borough as borough, brest.totalrestaurats as total from "+COLLECTION+" as r inner join (select cuisine, borough, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine, borough) as brest on r.cuisine = brest.cuisine and r.borough = brest.borough order by r.cuisine asc, r.borough asc limit 15").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(15, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Brooklyn\",\n" +
                "	\"total\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"African\",\n" +
                "	\"borough\": \"Bronx\",\n" +
                "	\"total\": 31\n" +
                "}]",toJson(results),false);
    }

    @Test
    public void twoSubqueriesJoinByOneAndTwoGetMaxOfTwoGroups() throws ParseException, JSONException, IOException {
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString("select r.cuisine as cuisine, r.borough as borough, trest.totalrestaurats as total, brest.totalrestaurats as local from "+COLLECTION+" as r inner join (select cuisine, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine) as trest on r.cuisine = trest.cuisine inner join (select cuisine, borough, count(*) as totalrestaurats from "+COLLECTION+" group by cuisine, borough) as brest on r.cuisine = brest.cuisine and r.borough = brest.borough order by cuisine asc, r.borough asc limit 15").build();
        QueryResultIterator<Document> distinctIterable = queryConverter.run(mongoDatabase);
        List<Document> results = Lists.newArrayList(distinctIterable);
        assertEquals(15, results.size());
        JSONAssert.assertEquals("[{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Brooklyn\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 1\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Manhattan\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 4\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"Afghan\",\n" +
                "	\"borough\": \"Queens\",\n" +
                "	\"total\": 14,\n" +
                "	\"local\": 9\n" +
                "},{\n" +
                "	\"cuisine\": \"African\",\n" +
                "	\"borough\": \"Bronx\",\n" +
                "	\"total\": 68,\n" +
                "	\"local\": 31\n" +
                "}]",toJson(results),false);
    }

    public enum Version implements IFeatureAwareVersion {

        V4_3_1("4.3.1", Feature.SYNC_DELAY, Feature.STORAGE_ENGINE, Feature.ONLY_64BIT,
                Feature.NO_CHUNKSIZE_ARG, Feature.MONGOS_CONFIGDB_SET_STYLE, Feature.NO_HTTP_INTERFACE_ARG,
                Feature.ONLY_WITH_SSL, Feature.ONLY_WINDOWS_2008_SERVER, Feature.NO_SOLARIS_SUPPORT,
                Feature.NO_BIND_IP_TO_LOCALHOST);


        private final String specificVersion;
        private EnumSet<Feature> features;

        Version(String vName, Feature...features) {
            this.specificVersion = vName;
            this.features = Feature.asSet(features);
        }

        @Override
        public boolean enabled(Feature feature) {
            return features.contains(feature);
        }

        @Override
        public EnumSet<Feature> getFeatures() {
            return EnumSet.copyOf(features);
        }

        @Override
        public String asInDownloadPath() {
            return specificVersion;
        }
    }

}
