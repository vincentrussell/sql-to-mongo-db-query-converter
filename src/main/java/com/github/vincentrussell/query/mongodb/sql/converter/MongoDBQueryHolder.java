package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;

import static org.apache.commons.lang.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private Document query = new Document();
    private Document projection = new Document();

    /**
     * Pojo to hold the MongoDB data
     * @param collection
     */
    public MongoDBQueryHolder(String collection){
        notNull(collection, "collection is null");
        this.collection = collection;
    }

    /**
     * Get the object used to create a projection
     * @return
     */
    public Document getProjection() {
        return projection;
    }

    /**
     * Get the object used to create a query
     * @return
     */
    public Document getQuery() {
        return query;
    }

    /**
     * Get the collection to run the query on
     * @return
     */
    public String getCollection() {
        return collection;
    }

    public void setQuery(Document query) {
        notNull(query, "query is null");
        this.query = query;
    }

    public void setProjection(Document projection) {
        notNull(projection, "projection is null");
        this.projection = projection;
    }
}
