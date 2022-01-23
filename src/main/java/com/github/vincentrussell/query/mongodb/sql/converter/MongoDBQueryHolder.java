package com.github.vincentrussell.query.mongodb.sql.converter;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.Validate.notNull;

public class MongoDBQueryHolder {
    private final String collection;
    private final SQLCommandType sqlCommandType;
    private Document query = new Document();
    private Document updateSet = new Document();
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
    /**
     * Aggregate previous steps.
     */
    private List<Document> prevSteps;
    private boolean requiresMultistepAggregation = false;

    /**
     * Pojo to hold the MongoDB data.
     *
     * @param collection     the collection that the query will be run on.
     * @param sqlCommandType the command type: like select or delete
     */
    public MongoDBQueryHolder(final String collection, final SQLCommandType sqlCommandType) {
        notNull(collection, "collection is null");
        this.collection = collection;
        this.sqlCommandType = sqlCommandType;
    }


    /**
     * Get the collection to run the query on.
     *
     * @return the collection to run the query on
     */
    public String getCollection() {
        return collection;
    }

    /**
     * get if distinct was used in the sql query.
     * @return true if distinct
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * set if there is distinct used in the sql query.
     * @param distinct true if the query should be be distinct on a particular field
     */
    public void setDistinct(final boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * Get the object used to create a query.
     *
     * @return the where clause section of the query in mongo formt
     */
    public Document getQuery() {
        return query;
    }

    /**
     * set they query from the where clause in the sql statement.
     * @param query the query in the form of {@link Document}
     */
    public void setQuery(final Document query) {
        notNull(query, "query is null");
        this.query = query;
    }


    /**
     * get the updateSet used for updates.
     * @return the updateSet
     */
    public Document getUpdateSet() {
        return updateSet;
    }

    /**
     *  set they updateSet which sets the fields to be updated.
     *  @param updateSet the updateSet in the form of {@link Document}
     */
    public void setUpdateSet(final Document updateSet) {
        notNull(updateSet, "updateSet is null");
        this.updateSet = updateSet;
    }

    /**
     * Get the column that is used in the distinct clause from the sql query or the projections
     * needed based on a group by sql statement.
     *
     * @return the fields to be returned by the query
     */
    public Document getProjection() {
        return projection;
    }

    /**
     * The column that is used in the distinct clause from the sql query or the projections
     * needed based on a group by sql statement.
     * @param projection the projection from the query based on the sql statement
     */
    public void setProjection(final Document projection) {
        notNull(projection, "projection is null");
        this.projection = projection;
    }

    /**
     * get the sort information for this query.
     * @return the sort information for this query
     */
    public Document getSort() {
        return sort;
    }

    /**
     * Set the sort information from the query based on the sql statement.
     * @param sort the sort information from the query
     */
    public void setSort(final Document sort) {
        notNull(sort, "sort is null");
        this.sort = sort;
    }

    /**
     * true if count(*) is used.
     * @return true if count(*) is used
     */
    public boolean isCountAll() {
        return countAll;
    }

    /**
     * set to true if count(*) is used.
     * @param countAll true if count(*) is used
     */
    public void setCountAll(final boolean countAll) {
        this.countAll = countAll;
    }

    /**
     * Set the fields that are used to group on.
     * @param groupBys the fields that are used to group on
     */
    public void setGroupBys(final List<String> groupBys) {
        this.groupBys = groupBys;
    }

    /**
     * get the fields that are used to group on.
     * @return the fields that are used to group on.
     */
    public List<String> getGroupBys() {
        return groupBys;
    }

    /**
     * get the having clause from the sql query.
     * @return the having clause from the sql query
     */
    public Document getHaving() {
        return having;
    }

    /**
     * set the having clause from the sql query.
     * @param having the having clause from the sql query
     */
    public void setHaving(final Document having) {
        notNull(having, "having is null");
        this.having = having;
    }

    /**
     * get the projections needed when using aliases for columns.
     * @return the projections needed when using aliases for columns
     */
    public Document getAliasProjection() {
        return aliasProjection;
    }

    /**
     * set the projections needed when using aliases for columns.
     * @param aliasProjection the projections needed when using aliases for columns
     */
    public void setAliasProjection(final Document aliasProjection) {
        this.aliasProjection = aliasProjection;
    }

    /**
     * get the limit used in the sql query.
     * @return the limit
     */
    public long getLimit() {
        return limit;
    }

    /**
     * set the limit used from the sql query.
     * @param limit the limit used for the sql query
     */
    public void setLimit(final long limit) {
        this.limit = limit;
    }

    /**
     * get the offset from the sql query.
     * @return the offset from the sql query
     */
    public long getOffset() {
        return offset;
    }

    /**
     * set the offset from the sql query.
     * @param offset the offset from the sql query
     */
    public void setOffset(final long offset) {
        this.offset = offset;
    }

    /**
     * Get the {@link SQLCommandType} for this query.
     * @return the sql command type
     */
    public SQLCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    /**
     * get the aggregation pipeline steps needed to perform a join.
     * @return the aggregation pipeline steps needed to perform a join
     */
    public List<Document> getJoinPipeline() {
        return joinPipeline;
    }

    /**
     * Set the aggregation pipeline steps needed to perform a join.
     * @param joinPipeline the aggregation pipeline steps needed to perform a join.
     */
    public void setJoinPipeline(final List<Document> joinPipeline) {
        this.joinPipeline = joinPipeline;
    }

    /**
     * Get the aggregation steps for this query.
     * @return the aggregation steps.
     */
    public List<Document> getPrevSteps() {
        return prevSteps;
    }

    /**
     * Will set the aggregation steps for this query.
     * @param prevSteps the aggregation steps for this query
     */
    public void setPrevSteps(final List<Document> prevSteps) {
        this.prevSteps = prevSteps;
    }

    /**
     * Set ff some aggregation is required by this query.
     * @param requiresMultistepAggregation ture if this sql query requires aggregation
     */
    public void setRequiresMultistepAggregation(final boolean requiresMultistepAggregation) {
        this.requiresMultistepAggregation = requiresMultistepAggregation;
    }

    /**
     * if some aggregation is required by this query.
     * @return if some aggregation is required by this query.
     */
    public boolean isRequiresMultistepAggregation() {
        return requiresMultistepAggregation;
    }

}

