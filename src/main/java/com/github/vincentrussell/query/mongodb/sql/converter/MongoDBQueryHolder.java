package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;


import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private final SQLCommandType sqlCommandType;
    private Document query = new Document();
    private Document projection = new Document();
    private Document sort = new Document();
    private Document aliasProjection = new Document();
    private List<Document> joinPipeline = new ArrayList<Document>();
    private boolean distinct = false;
    private boolean countAll = false;
    private List<String> groupBys = new ArrayList<>();
    private Document having = new Document();
    private long limit = -1;
    private long offset = -1;
    private List<Document> prevSteps;//Aggregate previous steps

    /**
     * Pojo to hold the MongoDB data
     * @param collection the collection that the query will be run on.
     * @param sqlCommandType the command type: like select or delete
     */
    public MongoDBQueryHolder(String collection, SQLCommandType sqlCommandType){
        notNull(collection, "collection is null");
        this.collection = collection;
        this.sqlCommandType = sqlCommandType;
    }

    /**
     * Get the object used to create a projection
     * @return the fields to be returned by the quer
     */
    public Document getProjection() {
        return projection;
    }

    /**
     * Get the object used to create a query
     * @return the where clause section of the query in mongo formt
     */
    public Document getQuery() {
        return query;
    }

    /**
     * Get the collection to run the query on
     * @return the collection to run the query on
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
    
	public Document getHaving() {
		return having;
    }

    public void setHaving(Document having) {
        notNull(having, "having is null");
        this.having = having;
    }
    
	public Document getAliasProjection() {
		return aliasProjection;
	}

	public void setAliasProjection(Document aliasProjection) {
		this.aliasProjection = aliasProjection;
	}

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }
    
    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

	public List<Document> getJoinPipeline() {
		return joinPipeline;
	}

	public void setJoinPipeline(List<Document> joinPipeline) {
		this.joinPipeline = joinPipeline;
	}

	public List<Document> getPrevSteps() {
		return prevSteps;
	}

	public void setPrevSteps(List<Document> prevSteps) {
		this.prevSteps = prevSteps;
	}
    
}
