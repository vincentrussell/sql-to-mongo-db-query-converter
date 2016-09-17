package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private Document query = new Document();
    private Document projection = new Document();
    private Document sort = new Document();
    private boolean distinct = false;
    private boolean countAll = false;
    private List<String> groupBys = new ArrayList<>();
    private long limit = -1;

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

    public boolean isDistinct() {
        return distinct;
    }

    public void setQuery(Document query) {
        notNull(query, "query is null");
        this.query = query;
    }

    public void setProjection(Document projection) {
        notNull(projection, "projection is null");
        this.projection = projection;
    }

    public Document getSort() {
        return sort;
    }

    public void setSort(Document sort) {
        notNull(sort, "sort is null");
        this.sort = sort;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isCountAll() {
        return countAll;
    }

    public void setCountAll(boolean countAll) {
        this.countAll = countAll;
    }

    public void setGroupBys(List<String> groupBys) {
        this.groupBys = groupBys;
    }

    public List<String> getGroupBys() {
        return groupBys;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }
}
