package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.holder.AliasHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.FromHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.SQLCommandInfoHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.processor.HavingClauseProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.processor.JoinProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.processor.WhereClauseProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.ExpVisitorEraseAliasTableBaseBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.WhereVisitorMatchAndLookupPipelineMatchBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.Validate.notNull;

public class QueryConverter {
    private final CCJSqlParser jSqlParser;
    private final Integer aggregationBatchSize;
    private final Boolean aggregationAllowDiskUse;
    private MongoDBQueryHolder mongoDBQueryHolder;

    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final FieldType defaultFieldType;
    private SQLCommandInfoHolder sqlCommandInfoHolder;

    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();

    /**
     * Create a QueryConverter not params for aggregate translation.
     *
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter() throws ParseException {
        fieldNameToFieldTypeMapping = Collections.<String, FieldType>emptyMap();
        defaultFieldType = FieldType.UNKNOWN;
        sqlCommandInfoHolder = null;
        mongoDBQueryHolder = null;
        jSqlParser = null;
        aggregationBatchSize = null;
        aggregationAllowDiskUse = null;
    }

    /**
     * Create a QueryConverter with a string.
     *
     * @param sql the sql statement
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final String sql) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8)),
                Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with a string.
     *
     * @param sql                         the sql statement
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final String sql,
                          final Map<String, FieldType> fieldNameToFieldTypeMapping) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8)), fieldNameToFieldTypeMapping, FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with a string.
     *
     * @param sql       sql string
     * @param fieldType the default {@link FieldType} to be used
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final String sql, final FieldType fieldType) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8)),
                Collections.<String, FieldType>emptyMap(), fieldType);
    }

    /**
     * Create a QueryConverter with a string.
     *
     * @param sql                         sql string
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @param defaultFieldType            defaultFieldType the default {@link FieldType} to be used
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final String sql, final Map<String, FieldType> fieldNameToFieldTypeMapping,
                          final FieldType defaultFieldType) throws ParseException {
        this(new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8)), fieldNameToFieldTypeMapping, defaultFieldType);
    }

    /**
     * Create a QueryConverter with a string.
     *
     * @param inputStream an input stream that has the sql statement in it
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final InputStream inputStream) throws ParseException {
        this(inputStream, Collections.<String, FieldType>emptyMap(), FieldType.UNKNOWN);
    }

    /**
     * Create a QueryConverter with an InputStream.
     *
     * @param inputStream                 an input stream that has the sql statement in it
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @param defaultFieldType            the default {@link FieldType} to be used
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final InputStream inputStream, final Map<String, FieldType> fieldNameToFieldTypeMapping,
                          final FieldType defaultFieldType) throws ParseException {
        this(inputStream, fieldNameToFieldTypeMapping, defaultFieldType, false, -1);
    }

    /**
     * Create a QueryConverter with an InputStream.
     *
     * @param inputStream                 an input stream that has the sql statement in it
     * @param fieldNameToFieldTypeMapping mapping for each field
     * @param defaultFieldType            the default {@link FieldType} to be used
     * @param aggregationAllowDiskUse     set whether or not disk use is allowed during aggregation
     * @param aggregationBatchSize        set the batch size for aggregation
     * @throws ParseException when the sql query cannot be parsed
     */
    @Deprecated
    public QueryConverter(final InputStream inputStream, final Map<String, FieldType> fieldNameToFieldTypeMapping,
                          final FieldType defaultFieldType, final Boolean aggregationAllowDiskUse,
                          final Integer aggregationBatchSize) throws ParseException {
        try {
            this.aggregationAllowDiskUse = aggregationAllowDiskUse;
            this.aggregationBatchSize = aggregationBatchSize;
            this.jSqlParser = new CCJSqlParser(new StreamProvider(inputStream, Charsets.UTF_8.name()));
            this.defaultFieldType = defaultFieldType != null ? defaultFieldType : FieldType.UNKNOWN;
            this.sqlCommandInfoHolder = SQLCommandInfoHolder.Builder
                    .create(defaultFieldType, fieldNameToFieldTypeMapping)
                    .setStatement(jSqlParser.Statement())
                    .build();
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping != null
                    ? fieldNameToFieldTypeMapping : Collections.<String, FieldType>emptyMap();

            net.sf.jsqlparser.parser.Token nextToken = jSqlParser.getNextToken();
            SqlUtils.isTrue(
                    isEmpty(nextToken.image) || ";".equals(nextToken.image),
                    "unable to parse complete sql string. one reason "
                            + "for this is the use of double equals (==)");

            this.mongoDBQueryHolder = getMongoQueryInternal(sqlCommandInfoHolder);
            validate();
        } catch (IOException e) {
            throw new ParseException(e);
        } catch (net.sf.jsqlparser.parser.ParseException e) {
            throw SqlUtils.convertParseException(e);
        }
    }

    private void validate() throws ParseException {
        List<SelectItem> selectItems = sqlCommandInfoHolder.getSelectItems();
        List<SelectItem> filteredItems = Lists.newArrayList(Iterables.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        try {
                            if (SelectExpressionItem.class.isInstance(selectItem)
                                    && Column.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                                return true;
                            }
                        } catch (NullPointerException e) {
                            return false;
                        }
                        return false;
                    }
                }));

        SqlUtils.isFalse((selectItems.size() > 1
                || SqlUtils.isSelectAll(selectItems))
                && sqlCommandInfoHolder.isDistinct(), "cannot run distinct one more than one column");
        SqlUtils.isFalse(sqlCommandInfoHolder.getGroupBys().size() == 0
                        && selectItems.size() != filteredItems.size() && !SqlUtils.isSelectAll(selectItems)
                        && !SqlUtils.isCountAll(selectItems)
                        && !sqlCommandInfoHolder.isTotalGroup(),
                "illegal expression(s) found in select clause.  Only column names supported");
    }

    /**
     * get the object that you need to submit a query.
     *
     * @return the {@link com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder}
     * that contains all that is needed to describe the query to be run.
     */
    public MongoDBQueryHolder getMongoQuery() {
        return mongoDBQueryHolder;
    }

    /**
     * Will convert the query into aggregation steps.
     * @param sqlCommandInfoHolder
     * @return the aggregation steps
     * @throws ParseException
     * @throws net.sf.jsqlparser.parser.ParseException
     */
    public List<Document> fromSQLCommandInfoHolderToAggregateSteps(final SQLCommandInfoHolder sqlCommandInfoHolder)
            throws ParseException, net.sf.jsqlparser.parser.ParseException {
        MongoDBQueryHolder mqueryHolder = getMongoQueryInternal(sqlCommandInfoHolder);
        return generateAggSteps(mqueryHolder, sqlCommandInfoHolder);
    }


    private MongoDBQueryHolder getMongoQueryInternal(final SQLCommandInfoHolder sqlCommandInfoHolder)
            throws ParseException, net.sf.jsqlparser.parser.ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = new MongoDBQueryHolder(
                sqlCommandInfoHolder.getBaseTableName(), sqlCommandInfoHolder.getSqlCommandType());
        Document document = new Document();
        //From Subquery
        if (sqlCommandInfoHolder.getFromHolder().getBaseFrom().getClass() == SubSelect.class) {
            mongoDBQueryHolder.setPrevSteps(fromSQLCommandInfoHolderToAggregateSteps(
                    (SQLCommandInfoHolder) sqlCommandInfoHolder.getFromHolder().getBaseSQLHolder()));
        }

        if (sqlCommandInfoHolder.isDistinct()) {
            document.put(sqlCommandInfoHolder.getSelectItems().get(0).toString(), 1);
            mongoDBQueryHolder.setProjection(document);
            mongoDBQueryHolder.setDistinct(sqlCommandInfoHolder.isDistinct());
        } else if (sqlCommandInfoHolder.getGroupBys().size() > 0) {
            List<String> groupBys = preprocessGroupBy(sqlCommandInfoHolder.getGroupBys(),
                    sqlCommandInfoHolder.getFromHolder());
            List<SelectItem> selects = preprocessSelect(sqlCommandInfoHolder.getSelectItems(),
                    sqlCommandInfoHolder.getFromHolder());
            if (sqlCommandInfoHolder.getGroupBys().size() > 0) {
                mongoDBQueryHolder.setGroupBys(groupBys);
            }
            mongoDBQueryHolder.setProjection(createProjectionsFromSelectItems(selects, groupBys));
            mongoDBQueryHolder.setAliasProjection(createAliasProjectionForGroupItems(selects, groupBys));
        } else if (sqlCommandInfoHolder.isTotalGroup()) {
            List<SelectItem> selects = preprocessSelect(sqlCommandInfoHolder.getSelectItems(),
                    sqlCommandInfoHolder.getFromHolder());
            Document d = createProjectionsFromSelectItems(selects, null);
            mongoDBQueryHolder.setProjection(d);
            mongoDBQueryHolder.setAliasProjection(createAliasProjectionForGroupItems(selects, null));
        } else if (!SqlUtils.isSelectAll(sqlCommandInfoHolder.getSelectItems())) {
            document.put("_id", 0);
            for (SelectItem selectItem : sqlCommandInfoHolder.getSelectItems()) {
                SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
                if (selectExpressionItem.getExpression() instanceof Column) {
                    Column c = (Column) selectExpressionItem.getExpression();
                    //If we found alias of base table we ignore it because basetable doesn't need alias, it's itself
                    String columnName = SqlUtils.removeAliasFromColumn(c, sqlCommandInfoHolder
                            .getFromHolder().getBaseAliasTable()).getColumnName();
                    Alias alias = selectExpressionItem.getAlias();
                    document.put((alias != null ? alias.getName() : columnName),
                            (alias != null ? "$" + columnName : 1));
                } else if (selectExpressionItem.getExpression() instanceof SubSelect) {
                    throw new ParseException("Unsupported subselect expression");
                } else {
                    throw new ParseException("Unsupported project expression");
                }
            }
            mongoDBQueryHolder.setProjection(document);
        }

        if (sqlCommandInfoHolder.isCountAll()) {
            mongoDBQueryHolder.setCountAll(sqlCommandInfoHolder.isCountAll());
        }

        if (sqlCommandInfoHolder.getJoins() != null) {
            mongoDBQueryHolder.setJoinPipeline(
                    JoinProcessor.toPipelineSteps(this,
                            sqlCommandInfoHolder.getFromHolder(),
                            sqlCommandInfoHolder.getJoins(), SqlUtils.cloneExpression(
                                    sqlCommandInfoHolder.getWhereClause())));
        }

        if (sqlCommandInfoHolder.getOrderByElements() != null && sqlCommandInfoHolder.getOrderByElements().size() > 0) {
            mongoDBQueryHolder.setSort(createSortInfoFromOrderByElements(
                    preprocessOrderBy(sqlCommandInfoHolder.getOrderByElements(), sqlCommandInfoHolder.getFromHolder()),
                    sqlCommandInfoHolder.getAliasHolder(), sqlCommandInfoHolder.getGroupBys()));
        }

        if (sqlCommandInfoHolder.getWhereClause() != null) {
            WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(defaultFieldType,
                    fieldNameToFieldTypeMapping);
            Expression preprocessedWhere = preprocessWhere(sqlCommandInfoHolder.getWhereClause(),
                    sqlCommandInfoHolder.getFromHolder());
            if (preprocessedWhere != null) {
                //can't be null because of where of joined tables
                mongoDBQueryHolder.setQuery((Document) whereClauseProcessor
                        .parseExpression(new Document(), preprocessedWhere, null));
            }
        }
        if (sqlCommandInfoHolder.getHavingClause() != null) {
            HavingClauseProcessor havingClauseProcessor = new HavingClauseProcessor(defaultFieldType,
                    fieldNameToFieldTypeMapping, sqlCommandInfoHolder.getAliasHolder());
            mongoDBQueryHolder.setHaving((Document) havingClauseProcessor.parseExpression(new Document(),
                    sqlCommandInfoHolder.getHavingClause(), null));
        }

        mongoDBQueryHolder.setOffset(sqlCommandInfoHolder.getOffset());
        mongoDBQueryHolder.setLimit(sqlCommandInfoHolder.getLimit());

        return mongoDBQueryHolder;
    }

    private Expression preprocessWhere(final Expression exp, final FromHolder tholder) {
        Expression returnValue = exp;
        if (sqlCommandInfoHolder.getJoins() != null && !sqlCommandInfoHolder.getJoins().isEmpty()) {
            ExpressionHolder partialWhereExpHolder = new ExpressionHolder(null);
            MutableBoolean haveOrExpression = new MutableBoolean(false);
            returnValue.accept(new WhereVisitorMatchAndLookupPipelineMatchBuilder(tholder.getBaseAliasTable(),
                    partialWhereExpHolder, haveOrExpression));
            if (haveOrExpression.booleanValue()) {
                //with or exp we can't use match first step
                return null;
            }
            returnValue = partialWhereExpHolder.getExpression();
        }
        if (returnValue != null) {
            returnValue.accept(new ExpVisitorEraseAliasTableBaseBuilder(tholder.getBaseAliasTable()));
        }
        return returnValue;
    }

    private List<OrderByElement> preprocessOrderBy(final List<OrderByElement> lord, final FromHolder tholder) {
        for (OrderByElement ord : lord) {
            ord.getExpression().accept(new ExpVisitorEraseAliasTableBaseBuilder(tholder.getBaseAliasTable()));
        }
        return lord;
    }

    private List<SelectItem> preprocessSelect(final List<SelectItem> lsel, final FromHolder tholder) {
        for (SelectItem sel : lsel) {
            sel.accept(new ExpVisitorEraseAliasTableBaseBuilder(tholder.getBaseAliasTable()));
        }
        return lsel;
    }

    private List<String> preprocessGroupBy(final List<String> lgroup, final FromHolder tholder) {
        List<String> lgroupEraseAlias = new LinkedList<>();
        for (String group : lgroup) {
            int index = group.indexOf(tholder.getBaseAliasTable() + ".");
            if (index != -1) {
                lgroupEraseAlias.add(group.substring(tholder.getBaseAliasTable().length() + 1));
            } else {
                lgroupEraseAlias.add(group);
            }
        }
        return lgroupEraseAlias;
    }

    private Document createSortInfoFromOrderByElements(final List<OrderByElement> orderByElements,
                                                       final AliasHolder aliasHolder,
                                                       final List<String> groupBys) throws ParseException {
        if (orderByElements.size() == 0) {
            return new Document();
        }

        final List<OrderByElement> functionItems = Lists.newArrayList(Iterables.filter(orderByElements,
                new Predicate<OrderByElement>() {
                    @Override
                    public boolean apply(final OrderByElement orderByElement) {
                        try {
                            if (Function.class.isInstance(orderByElement.getExpression())) {
                                return true;
                            }
                        } catch (NullPointerException e) {
                            return false;
                        }
                        return false;
                    }
                }));
        final List<OrderByElement> nonFunctionItems = Lists.newArrayList(Collections2.filter(orderByElements,
                new Predicate<OrderByElement>() {
                    @Override
                    public boolean apply(final OrderByElement orderByElement) {
                        return !functionItems.contains(orderByElement);
                    }

                }));

        Document sortItems = new Document();
        for (OrderByElement orderByElement : orderByElements) {
            if (nonFunctionItems.contains(orderByElement)) {
                String sortField = SqlUtils.getStringValue(orderByElement.getExpression());
                String projectField = aliasHolder.getFieldFromAliasOrField(sortField);
                if (!groupBys.isEmpty()) {

                    if (!SqlUtils.isAggregateExpression(projectField)) {
                        if (groupBys.size() > 1) {
                            projectField = "_id." + projectField.replaceAll("\\.", "_");
                        } else {
                            projectField = "_id";
                        }
                    } else {
                        projectField = sortField;
                    }
                }
                sortItems.put(projectField, orderByElement.isAsc() ? 1 : -1);
            } else {
                Function function = (Function) orderByElement.getExpression();
                String sortKey;
                String alias = aliasHolder.getAliasFromFieldExp(function.toString());
                if (alias != null && !alias.equals(function.toString())) {
                    sortKey = alias;
                } else {
                    Document parseFunctionDocument = new Document();
                    parseFunctionForAggregation(function, parseFunctionDocument,
                            Collections.<String>emptyList(), null);
                    sortKey = Iterables.get(parseFunctionDocument.keySet(), 0);
                }
                sortItems.put(sortKey, orderByElement.isAsc() ? 1 : -1);
            }
        }

        return sortItems;
    }

    private Document createProjectionsFromSelectItems(final List<SelectItem> selectItems,
                                                      final List<String> groupBys) throws ParseException {
        Document document = new Document();
        if (selectItems.size() == 0) {
            return document;
        }

        final List<SelectItem> functionItems = Lists.newArrayList(Iterables.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        try {
                            if (SelectExpressionItem.class.isInstance(selectItem)
                                    && Function.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                                return true;
                            }
                        } catch (NullPointerException e) {
                            return false;
                        }
                        return false;
                    }
                }));
        final List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        return !functionItems.contains(selectItem);
                    }

                }));

        Document idDocument = new Document();
        for (SelectItem selectItem : nonFunctionItems) {
            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
            Column column = (Column) selectExpressionItem.getExpression();
            String columnName = SqlUtils.getStringValue(column);
            idDocument.put(columnName.replaceAll("\\.", "_"), "$" + columnName);
        }

        if (!idDocument.isEmpty()) {
            document.append("_id", idDocument.size() == 1 ? Iterables.get(idDocument.values(), 0) : idDocument);
        }

        for (SelectItem selectItem : functionItems) {
            Function function = (Function) ((SelectExpressionItem) selectItem).getExpression();
            parseFunctionForAggregation(function, document, groupBys, ((SelectExpressionItem) selectItem).getAlias());
        }

        return document;
    }

    private Document createAliasProjectionForGroupItems(final List<SelectItem> selectItems,
                                                        final List<String> groupBys) throws ParseException {
        Document document = new Document();

        final List<SelectItem> functionItems = Lists.newArrayList(Iterables.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        try {
                            if (SelectExpressionItem.class.isInstance(selectItem)
                                    && Function.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                                return true;
                            }
                        } catch (NullPointerException e) {
                            return false;
                        }
                        return false;
                    }
                }));
        final List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        return !functionItems.contains(selectItem);
                    }

                }));

        if (nonFunctionItems.size() == 1) {
            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) nonFunctionItems.get(0));
            Column column = (Column) selectExpressionItem.getExpression();
            String columnName = SqlUtils.getStringValue(column);
            Alias alias = selectExpressionItem.getAlias();
            String nameOrAlias = (alias != null ? alias.getName() : columnName);
            document.put(nameOrAlias, "$_id");
        } else {
            for (SelectItem selectItem : nonFunctionItems) {
                SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
                Column column = (Column) selectExpressionItem.getExpression();
                String columnName = SqlUtils.getStringValue(column);
                Alias alias = selectExpressionItem.getAlias();
                String nameOrAlias = (alias != null ? alias.getName() : columnName);
                document.put(nameOrAlias, "$_id." + columnName.replaceAll("\\.", "_"));
            }
        }

        for (SelectItem selectItem : functionItems) {
            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
            Function function = (Function) selectExpressionItem.getExpression();
            Alias alias = selectExpressionItem.getAlias();
            document.put(SqlUtils.generateAggField(function, alias), 1);
        }

        document.put("_id", 0);

        return document;
    }

    private void parseFunctionForAggregation(final Function function, final Document document,
                                             final List<String> groupBys, final Alias alias) throws ParseException {
        String op = function.getName().toLowerCase();
        String aggField = SqlUtils.generateAggField(function, alias);
        switch (op) {
            case "count":
                document.put(aggField, new Document("$sum", 1));
                break;
            case "sum":
            case "min":
            case "max":
            case "avg":
                createFunction(op, aggField, document, "$" + SqlUtils.getFieldFromFunction(function));
                break;
            default:
                throw new ParseException("could not understand function:" + function.getName());
        }
    }

    private void createFunction(final String functionName, final String aggField,
                                final Document document, final Object value) {
        document.put(aggField, new Document("$" + functionName, value));
    }


    /**
     * Build a mongo shell statement with the code to run the specified query.
     *
     * @param outputStream the {@link java.io.OutputStream} to write the data to
     * @throws IOException when there is an issue writing to the {@link java.io.OutputStream}
     */
    public void write(final OutputStream outputStream) throws IOException {
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();
        boolean isFindQuery = false;
        if (mongoDBQueryHolder.isDistinct()) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".distinct(", outputStream);
            IOUtils.write("\"" + getDistinctFieldName(mongoDBQueryHolder) + "\"", outputStream);
            IOUtils.write(" , ", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson(RELAXED)), outputStream);
        } else if (sqlCommandInfoHolder.isCountAll() && !isAggregate(mongoDBQueryHolder)) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".count(", outputStream);
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson(RELAXED)), outputStream);
        } else if (isAggregate(mongoDBQueryHolder)) {
            IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".aggregate(", outputStream);
            IOUtils.write("[", outputStream);

            IOUtils.write(Joiner.on(",").join(Lists.transform(
                    generateAggSteps(mongoDBQueryHolder, sqlCommandInfoHolder),
                    new com.google.common.base.Function<Document, String>() {
                        @Override
                        public String apply(final Document document) {
                            return prettyPrintJson(document.toJson(RELAXED));
                        }
                    })), outputStream);
            IOUtils.write("]", outputStream);

            Document options = new Document();
            if (aggregationAllowDiskUse != null) {
                options.put("allowDiskUse", aggregationAllowDiskUse);
            }

            if (aggregationBatchSize != null) {
                options.put("cursor", new Document("batchSize", aggregationBatchSize));
            }

            if (options.size() > 0) {
                IOUtils.write(",", outputStream);
                IOUtils.write(prettyPrintJson(options.toJson(RELAXED)), outputStream);
            }


        } else {
            if (sqlCommandInfoHolder.getSqlCommandType() == SQLCommandType.SELECT) {
                isFindQuery = true;
                IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".find(", outputStream);
            } else if (sqlCommandInfoHolder.getSqlCommandType() == SQLCommandType.DELETE) {
                IOUtils.write("db." + mongoDBQueryHolder.getCollection() + ".remove(", outputStream);
            }
            IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getQuery().toJson(RELAXED)), outputStream);
            if (mongoDBQueryHolder.getProjection() != null && mongoDBQueryHolder.getProjection().size() > 0
                    && sqlCommandInfoHolder.getSqlCommandType() == SQLCommandType.SELECT) {
                IOUtils.write(" , ", outputStream);
                IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getProjection().toJson(RELAXED)), outputStream);
            }
        }
        IOUtils.write(")", outputStream);

        if (isFindQuery) {
            if (mongoDBQueryHolder.getSort() != null && mongoDBQueryHolder.getSort().size() > 0) {
                IOUtils.write(".sort(", outputStream);
                IOUtils.write(prettyPrintJson(mongoDBQueryHolder.getSort().toJson(RELAXED)), outputStream);
                IOUtils.write(")", outputStream);
            }

            if (mongoDBQueryHolder.getOffset() != -1) {
                IOUtils.write(".skip(", outputStream);
                IOUtils.write(mongoDBQueryHolder.getOffset() + "", outputStream);
                IOUtils.write(")", outputStream);
            }

            if (mongoDBQueryHolder.getLimit() != -1) {
                IOUtils.write(".limit(", outputStream);
                IOUtils.write(mongoDBQueryHolder.getLimit() + "", outputStream);
                IOUtils.write(")", outputStream);
            }
        }
    }

    private boolean isAggregate(final MongoDBQueryHolder mongoDBQueryHolder) {
        return (sqlCommandInfoHolder.getAliasHolder() != null
                && !sqlCommandInfoHolder.getAliasHolder().isEmpty())
                || sqlCommandInfoHolder.getGroupBys().size() > 0
                || (sqlCommandInfoHolder.getJoins() != null && sqlCommandInfoHolder.getJoins().size() > 0)
                || (mongoDBQueryHolder.getPrevSteps() != null && !mongoDBQueryHolder.getPrevSteps().isEmpty())
                || (sqlCommandInfoHolder.isTotalGroup() && !SqlUtils.isCountAll(sqlCommandInfoHolder.getSelectItems()));
    }

    private String getDistinctFieldName(final MongoDBQueryHolder mongoDBQueryHolder) {
        return Iterables.get(mongoDBQueryHolder.getProjection().keySet(), 0);
    }

    /**
     * @param mongoDatabase the database to run the query against.
     * @param <T>           variable based on the type of query run.
     * @return When query does a find will return QueryResultIterator&lt;{@link org.bson.Document}&gt;
     * When query does a count will return a Long
     * When query does a distinct will return QueryResultIterator&lt;{@link java.lang.String}&gt;
     * @throws ParseException when the sql query cannot be parsed
     */
    @SuppressWarnings("unchecked")
    public <T> T run(final MongoDatabase mongoDatabase) throws ParseException {
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();

        MongoCollection mongoCollection = mongoDatabase.getCollection(mongoDBQueryHolder.getCollection());

        if (SQLCommandType.SELECT.equals(mongoDBQueryHolder.getSqlCommandType())) {

            if (mongoDBQueryHolder.isDistinct()) {
                return (T) new QueryResultIterator<>(mongoCollection.distinct(
                        getDistinctFieldName(mongoDBQueryHolder), mongoDBQueryHolder.getQuery(), String.class));
            } else if (sqlCommandInfoHolder.isCountAll() && !isAggregate(mongoDBQueryHolder)) {
                return (T) Long.valueOf(mongoCollection.count(mongoDBQueryHolder.getQuery()));
            } else if (isAggregate(mongoDBQueryHolder)) {

                AggregateIterable aggregate = mongoCollection.aggregate(
                        generateAggSteps(mongoDBQueryHolder, sqlCommandInfoHolder));

                if (aggregationAllowDiskUse != null) {
                    aggregate.allowDiskUse(aggregationAllowDiskUse);
                }

                if (aggregationBatchSize != null) {
                    aggregate.batchSize(aggregationBatchSize);
                }

                return (T) new QueryResultIterator<>(aggregate);
            } else {
                FindIterable findIterable = mongoCollection.find(mongoDBQueryHolder.getQuery())
                        .projection(mongoDBQueryHolder.getProjection());
                if (mongoDBQueryHolder.getSort() != null && mongoDBQueryHolder.getSort().size() > 0) {
                    findIterable.sort(mongoDBQueryHolder.getSort());
                }
                if (mongoDBQueryHolder.getOffset() != -1) {
                    findIterable.skip((int) mongoDBQueryHolder.getOffset());
                }
                if (mongoDBQueryHolder.getLimit() != -1) {
                    findIterable.limit((int) mongoDBQueryHolder.getLimit());
                }

                return (T) new QueryResultIterator<>(findIterable);
            }
        } else if (SQLCommandType.DELETE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            DeleteResult deleteResult = mongoCollection.deleteMany(mongoDBQueryHolder.getQuery());
            return (T) ((Long) deleteResult.getDeletedCount());
        } else {
            throw new UnsupportedOperationException("SQL command type not supported");
        }
    }

    //Set up start pipeline, from other steps, subqueries, ...
    private List<Document> setUpStartPipeline(final MongoDBQueryHolder mongoDBQueryHolder) {
        List<Document> documents = mongoDBQueryHolder.getPrevSteps();
        if (documents == null || documents.isEmpty()) {
            documents = new LinkedList<Document>();
        }
        return documents;
    }

    private List<Document> generateAggSteps(final MongoDBQueryHolder mongoDBQueryHolder,
                                           final SQLCommandInfoHolder sqlCommandInfoHolder) {

        List<Document> documents = setUpStartPipeline(mongoDBQueryHolder);

        if (mongoDBQueryHolder.getQuery() != null && mongoDBQueryHolder.getQuery().size() > 0) {
            documents.add(new Document("$match", mongoDBQueryHolder.getQuery()));
        }
        if (sqlCommandInfoHolder.getJoins() != null && !sqlCommandInfoHolder.getJoins().isEmpty()) {
            documents.addAll(mongoDBQueryHolder.getJoinPipeline());
        }
        if (!sqlCommandInfoHolder.getGroupBys().isEmpty() || sqlCommandInfoHolder.isTotalGroup()) {
            if (mongoDBQueryHolder.getProjection().get("_id") == null) {
                //Generate _id with empty document
                Document dgroup = new Document();
                dgroup.put("_id", new Document());
                for (Entry<String, Object> keyValue : mongoDBQueryHolder.getProjection().entrySet()) {
                    if (!keyValue.getKey().equals("_id")) {
                        dgroup.put(keyValue.getKey(), keyValue.getValue());
                    }
                }
                documents.add(new Document("$group", dgroup));
            } else {
                documents.add(new Document("$group", mongoDBQueryHolder.getProjection()));
            }

        }
        if (mongoDBQueryHolder.getHaving() != null && mongoDBQueryHolder.getHaving().size() > 0) {
            documents.add(new Document("$match", mongoDBQueryHolder.getHaving()));
        }
        if (mongoDBQueryHolder.getSort() != null && mongoDBQueryHolder.getSort().size() > 0) {
            documents.add(new Document("$sort", mongoDBQueryHolder.getSort()));
        }
        if (mongoDBQueryHolder.getOffset() != -1) {
            documents.add(new Document("$skip", mongoDBQueryHolder.getOffset()));
        }
        if (mongoDBQueryHolder.getLimit() != -1) {
            documents.add(new Document("$limit", mongoDBQueryHolder.getLimit()));
        }

        Document aliasProjection = mongoDBQueryHolder.getAliasProjection();
        if (!aliasProjection.isEmpty()) {
            //Alias Group by
            documents.add(new Document("$project", aliasProjection));
        }

        if (sqlCommandInfoHolder.getGroupBys().isEmpty() && !sqlCommandInfoHolder.isTotalGroup()
                && !mongoDBQueryHolder.getProjection().isEmpty()) {
            //Alias no group
            Document projection = mongoDBQueryHolder.getProjection();
            documents.add(new Document("$project", projection));
        }

        return documents;
    }

    private static String toJson(final List<Document> documents) throws IOException {
        StringWriter stringWriter = new StringWriter();
        IOUtils.write("[", stringWriter);
        IOUtils.write(Joiner.on(",").join(Lists.transform(documents,
                new com.google.common.base.Function<Document, String>() {
                    @Override
                    public String apply(@Nonnull final Document document) {
                        return document.toJson(RELAXED);
                    }
                })), stringWriter);
        IOUtils.write("]", stringWriter);
        return stringWriter.toString();
    }


    private String prettyPrintJson(final String json) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(json);
        return gson.toJson(je);
    }

    /**
     * Builder for {@link QueryConverter}.
     */
    public static class Builder {

        private Boolean aggregationAllowDiskUse = null;
        private Integer aggregationBatchSize = null;
        private InputStream inputStream;
        private Map<String, FieldType> fieldNameToFieldTypeMapping = new HashMap<>();
        private FieldType defaultFieldType = FieldType.UNKNOWN;

        /**
         * set the inputstream that contains the sql string.
         * @param inputStream
         * @return the builder
         */
        public Builder sqlInputStream(final InputStream inputStream) {
            notNull(inputStream);
            this.inputStream = inputStream;
            return this;
        }

        /**
         * set the sql string.
         * @param sql
         * @return the builder
         */
        public Builder sqlString(final String sql) {
            notNull(sql);
            this.inputStream = new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8));
            return this;
        }

        /**
         * set the column to {@link FieldType} mapping.
         * @param fieldNameToFieldTypeMapping
         * @return the builder
         */
        public Builder fieldNameToFieldTypeMapping(final Map<String, FieldType> fieldNameToFieldTypeMapping) {
            notNull(fieldNameToFieldTypeMapping);
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
            return this;
        }

        /**
         * set the default {@link FieldType}.
         * @param defaultFieldType
         * @return builder
         */
        public Builder defaultFieldType(final FieldType defaultFieldType) {
            notNull(defaultFieldType);
            this.defaultFieldType = defaultFieldType;
            return this;
        }

        /**
         * set whether or not aggregation is allowed to use disk use.
         * @param aggregationAllowDiskUse
         * @return the builder
         */
        public Builder aggregationAllowDiskUse(final Boolean aggregationAllowDiskUse) {
            notNull(aggregationAllowDiskUse);
            this.aggregationAllowDiskUse = aggregationAllowDiskUse;
            return this;
        }

        /**
         * set the batch size for aggregation.
         * @param aggregationBatchSize
         * @return the builder
         */
        public Builder aggregationBatchSize(final Integer aggregationBatchSize) {
            notNull(aggregationBatchSize);
            this.aggregationBatchSize = aggregationBatchSize;
            return this;
        }

        /**
         * build the {@link QueryConverter}.
         * @return the {@link QueryConverter}
         * @throws ParseException
         */
        public QueryConverter build() throws ParseException {
            return new QueryConverter(inputStream, fieldNameToFieldTypeMapping,
                    defaultFieldType, aggregationAllowDiskUse, aggregationBatchSize);
        }
    }


}
