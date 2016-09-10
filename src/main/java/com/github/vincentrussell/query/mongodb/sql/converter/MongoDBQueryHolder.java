package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;

import static org.apache.commons.lang.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private Document query = new Document();
    private Document projection = new Document();

    public MongoDBQueryHolder(String collection){
        notNull(collection, "collection is null");
        this.collection = collection;
    };

    public Document getProjection() {
        return projection;
    }

    public Document getQuery() {
        return query;
    }

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
