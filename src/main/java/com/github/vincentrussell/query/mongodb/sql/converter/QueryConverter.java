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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.UpdateSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.Validate.notNull;

/**
 * Main class responsible for query conversion.
 */
public final class QueryConverter {
    private final CCJSqlParser jSqlParser;
    private final Integer aggregationBatchSize;
    private final Boolean aggregationAllowDiskUse;
    private final MongoDBQueryHolder mongoDBQueryHolder;

    private final Map<String, FieldType> fieldNameToFieldTypeMapping;
    private final FieldType defaultFieldType;
    private final SQLCommandInfoHolder sqlCommandInfoHolder;

    private static final JsonWriterSettings RELAXED = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();



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
    private QueryConverter(final InputStream inputStream, final Map<String, FieldType> fieldNameToFieldTypeMapping,
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
                    ? fieldNameToFieldTypeMapping : Collections.emptyMap();

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
                            if (SelectExpressionItem.class.isInstance(selectItem)) {
//                                    && Column.class.isInstance(((SelectExpressionItem) selectItem).getExpression())) {
                                return true;
                            }
                        } catch (NullPointerException e) {
                            return false;
                        }
                        return false;
                    }

                    @Override
                    public boolean test(final SelectItem input) {
                        return apply(input);
                    }

                }));

        SqlUtils.isFalse((selectItems.size() > 1
                || SqlUtils.isSelectAll(selectItems))
                && sqlCommandInfoHolder.isDistinct(), "cannot run distinct one more than one column");
        SqlUtils.isFalse(sqlCommandInfoHolder.getGroupBys().size() == 0
                        && selectItems.size() != filteredItems.size()
                        && !SqlUtils.isSelectAll(selectItems)
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
     * @param sqlCommandInfoHolder the {@link SQLCommandInfoHolder}
     * @return the aggregation steps
     * @throws ParseException if there is an issue parsing the sql
     * @throws net.sf.jsqlparser.parser.ParseException if there is an issue parsing the sql
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
            mongoDBQueryHolder.setRequiresMultistepAggregation(true);
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
            AliasProjectionForGroupItems aliasProjectionForGroupItems = createAliasProjectionForGroupItems(
                    selects, groupBys);
            mongoDBQueryHolder.setAliasProjection(aliasProjectionForGroupItems.getDocument());
            mongoDBQueryHolder.setRequiresMultistepAggregation(true);
        } else if (sqlCommandInfoHolder.isTotalGroup()) {
            List<SelectItem> selects = preprocessSelect(sqlCommandInfoHolder.getSelectItems(),
                    sqlCommandInfoHolder.getFromHolder());
            Document d = createProjectionsFromSelectItems(selects, null);
            mongoDBQueryHolder.setProjection(d);
            AliasProjectionForGroupItems aliasProjectionForGroupItems = createAliasProjectionForGroupItems(
                    selects, null);
            sqlCommandInfoHolder.getAliasHolder().combine(aliasProjectionForGroupItems.getFieldToAliasMapping());
            mongoDBQueryHolder.setAliasProjection(aliasProjectionForGroupItems.getDocument());
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
                } else if (selectExpressionItem.getExpression() instanceof Function) {
                    Function f = (Function) selectExpressionItem.getExpression();
                    String columnName = f.toString();
                    Alias alias = selectExpressionItem.getAlias();
                    String key = (alias != null ? alias.getName() : columnName);
                    Document functionDoc = (Document) recurseFunctions(new Document(), f,
                            defaultFieldType, fieldNameToFieldTypeMapping);
                    document.put(key, functionDoc);
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
            mongoDBQueryHolder.setRequiresMultistepAggregation(true);
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
                    fieldNameToFieldTypeMapping, mongoDBQueryHolder.isRequiresMultistepAggregation());
            Expression preprocessedWhere = preprocessWhere(sqlCommandInfoHolder.getWhereClause(),
                    sqlCommandInfoHolder.getFromHolder());
            if (preprocessedWhere != null) {
                //can't be null because of where of joined tables
                mongoDBQueryHolder.setQuery((Document) whereClauseProcessor
                        .parseExpression(new Document(), preprocessedWhere, null));
            }

            if (SQLCommandType.UPDATE.equals(sqlCommandInfoHolder.getSqlCommandType())) {
                Document updateSetDoc = new Document();
                for (UpdateSet updateSet : Iterables.filter(sqlCommandInfoHolder.getUpdateSets(),
                        new Predicate<UpdateSet>() {
                    @Override
                    public boolean apply(@org.checkerframework.checker.nullness.qual.Nullable final UpdateSet input) {
                        return !NullValue.class.isInstance(input.getExpressions().get(0));
                    }
                })) {
                    SqlUtils.isTrue(updateSet.getColumns().size() == 1,
                            "more than one column in an update set is not supported");
                    SqlUtils.isTrue(updateSet.getExpressions().size() == 1,
                            "more than one expression in an update set is not supported");
                    updateSetDoc.put(SqlUtils.getColumnNameFromColumn(updateSet.getColumns().get(0)),
                            SqlUtils.getNormalizedValue(updateSet.getExpressions().get(0), null,
                                    defaultFieldType, fieldNameToFieldTypeMapping,
                                    sqlCommandInfoHolder.getAliasHolder(), null));
                }
                mongoDBQueryHolder.setUpdateSet(updateSetDoc);
                List<String> unsets = new ArrayList<>();
                for (UpdateSet updateSet : Iterables.filter(sqlCommandInfoHolder.getUpdateSets(),
                        new Predicate<UpdateSet>() {
                    @Override
                    public boolean apply(@org.checkerframework.checker.nullness.qual.Nullable final UpdateSet input) {
                        return NullValue.class.isInstance(input.getExpressions().get(0));
                    }
                })) {
                    SqlUtils.isTrue(updateSet.getColumns().size() == 1,
                            "more than one column in an update set is not supported");
                    SqlUtils.isTrue(updateSet.getExpressions().size() == 1,
                            "more than one expression in an update set is not supported");
                    unsets.add(SqlUtils.getColumnNameFromColumn(updateSet.getColumns().get(0)));
                }
                mongoDBQueryHolder.setFieldsToUnset(unsets);
            }
        }
        if (sqlCommandInfoHolder.getHavingClause() != null) {
            HavingClauseProcessor havingClauseProcessor = new HavingClauseProcessor(defaultFieldType,
                    fieldNameToFieldTypeMapping, sqlCommandInfoHolder.getAliasHolder(),
                    mongoDBQueryHolder.isRequiresMultistepAggregation());
            mongoDBQueryHolder.setHaving((Document) havingClauseProcessor.parseExpression(new Document(),
                    sqlCommandInfoHolder.getHavingClause(), null));
        }

        mongoDBQueryHolder.setOffset(sqlCommandInfoHolder.getOffset());
        mongoDBQueryHolder.setLimit(sqlCommandInfoHolder.getLimit());

        return mongoDBQueryHolder;
    }

    protected Object recurseFunctions(final Document query, final Object object,
                                      final FieldType defaultFieldType,
                                      final Map<String, FieldType> fieldNameToFieldTypeMapping) throws ParseException {
        if (Function.class.isInstance(object)) {
            Function function = (Function) object;
            query.put("$" + SqlUtils.translateFunctionName(function.getName()),
                    recurseFunctions(new Document(), function.getParameters(),
                            defaultFieldType, fieldNameToFieldTypeMapping));
        } else if (ExpressionList.class.isInstance(object)) {
            ExpressionList expressionList = (ExpressionList) object;
            List<Object> objectList = new ArrayList<>();
            for (Expression expression : expressionList.getExpressions()) {
                objectList.add(recurseFunctions(new Document(), expression,
                        defaultFieldType, fieldNameToFieldTypeMapping));
            }
            return objectList.size() == 1 ? objectList.get(0) : objectList;
        } else if (Expression.class.isInstance(object)) {
            Object normalizedValue = SqlUtils.getNormalizedValue((Expression) object, null,
                    defaultFieldType, fieldNameToFieldTypeMapping, null);
            if (Column.class.isInstance(object)) {
                return "$" + ((String)normalizedValue);
            } else {
                return normalizedValue;
            }
        }

        return query.isEmpty() ? null : query;
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

                    @Override
                    public boolean test(final OrderByElement input) {
                        return apply(input);
                    }
                }));
        final List<OrderByElement> nonFunctionItems = Lists.newArrayList(Collections2.filter(orderByElements,
                new Predicate<OrderByElement>() {
                    @Override
                    public boolean apply(final OrderByElement orderByElement) {
                        return !functionItems.contains(orderByElement);
                    }

                    @Override
                    public boolean test(final OrderByElement input) {
                        return apply(input);
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

                    @Override
                    public boolean test(final SelectItem input) {
                        return apply(input);
                    }
                }));
        final List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        return !functionItems.contains(selectItem);
                    }

                    @Override
                    public boolean test(final SelectItem input) {
                        return apply(input);
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

    private AliasProjectionForGroupItems createAliasProjectionForGroupItems(final List<SelectItem> selectItems,
                                                        final List<String> groupBys) throws ParseException {

        AliasProjectionForGroupItems aliasProjectionForGroupItems = new AliasProjectionForGroupItems();

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

                    @Override
                    public boolean test(final SelectItem input) {
                        return apply(input);
                    }

                }));
        final List<SelectItem> nonFunctionItems = Lists.newArrayList(Collections2.filter(selectItems,
                new Predicate<SelectItem>() {
                    @Override
                    public boolean apply(final SelectItem selectItem) {
                        return !functionItems.contains(selectItem);
                    }

                    @Override
                    public boolean test(final SelectItem input) {
                        return apply(input);
                    }

                }));

        if (nonFunctionItems.size() == 1) {
            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) nonFunctionItems.get(0));
            Column column = (Column) selectExpressionItem.getExpression();
            String columnName = SqlUtils.getStringValue(column);
            Alias alias = selectExpressionItem.getAlias();
            String nameOrAlias = (alias != null ? alias.getName() : columnName);
            aliasProjectionForGroupItems.getDocument().put(nameOrAlias, "$_id");
        } else {
            for (SelectItem selectItem : nonFunctionItems) {
                SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
                Column column = (Column) selectExpressionItem.getExpression();
                String columnName = SqlUtils.getStringValue(column);
                Alias alias = selectExpressionItem.getAlias();
                String nameOrAlias = (alias != null ? alias.getName() : columnName);
                aliasProjectionForGroupItems.getDocument().put(nameOrAlias,
                        "$_id." + columnName.replaceAll("\\.", "_"));
            }
        }

        for (SelectItem selectItem : functionItems) {
            SelectExpressionItem selectExpressionItem = ((SelectExpressionItem) selectItem);
            Function function = (Function) selectExpressionItem.getExpression();
            Alias alias = selectExpressionItem.getAlias();
            Entry<String, String> fieldToAliasMapping = SqlUtils.generateAggField(function, alias);
            String aliasedField = fieldToAliasMapping.getValue();
            aliasProjectionForGroupItems.putAlias(fieldToAliasMapping.getKey(), fieldToAliasMapping.getValue());
            aliasProjectionForGroupItems.getDocument().put(aliasedField, 1);
        }

        aliasProjectionForGroupItems.getDocument().put("_id", 0);

        return aliasProjectionForGroupItems;
    }

    private void parseFunctionForAggregation(final Function function, final Document document,
                                             final List<String> groupBys, final Alias alias) throws ParseException {
        String op = function.getName().toLowerCase();
        String aggField = SqlUtils.generateAggField(function, alias).getValue();
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
        Document queryDocument = getQueryAsDocument();
        String collectionName = queryDocument.getString("collection");
        boolean isAggregation = queryDocument.get("query") != null && List.class.isInstance(queryDocument.get("query"));
        boolean isFindQuery = false;
        if (queryDocument.get("distinct") != null) {
            IOUtils.write("db." + collectionName + ".distinct(", outputStream, StandardCharsets.UTF_8);
            IOUtils.write("\"" + queryDocument.get("distinct") + "\"", outputStream, StandardCharsets.UTF_8);
            IOUtils.write(" , ", outputStream, StandardCharsets.UTF_8);
            IOUtils.write(prettyPrintJson(((Document) queryDocument.get("query")).toJson(RELAXED)),
                    outputStream, StandardCharsets.UTF_8);
        } else if (Boolean.TRUE.equals(queryDocument.getBoolean("countAll")) && !isAggregation) {
            IOUtils.write("db." + collectionName + ".count(", outputStream, StandardCharsets.UTF_8);
            IOUtils.write(prettyPrintJson(((Document) queryDocument.get("query")).toJson(RELAXED)), outputStream,
                    StandardCharsets.UTF_8);
        } else {
            if (isAggregation) {
                IOUtils.write("db." + collectionName + ".aggregate(", outputStream, StandardCharsets.UTF_8);
                IOUtils.write("[", outputStream, StandardCharsets.UTF_8);

                IOUtils.write(Joiner.on(",").join(Lists.transform(
                        queryDocument.getList("query", Document.class),
                        new com.google.common.base.Function<Document, String>() {
                            @Override
                            public String apply(@Nonnull final Document document) {
                                return prettyPrintJson(document.toJson(RELAXED));
                            }
                        })), outputStream, StandardCharsets.UTF_8);
                IOUtils.write("]", outputStream, StandardCharsets.UTF_8);

                Document options = (Document) queryDocument.get("options");
                if (options != null && options.size() > 0) {
                    IOUtils.write(",", outputStream, StandardCharsets.UTF_8);
                    IOUtils.write(prettyPrintJson(options.toJson(RELAXED)), outputStream, StandardCharsets.UTF_8);
                }


            } else {
                SQLCommandType sqlCommandType = SQLCommandType.valueOf(
                        firstNonNull(queryDocument.get("commandType"), SQLCommandType.SELECT.name()).toString());
                if (SQLCommandType.SELECT.equals(sqlCommandType)) {
                    isFindQuery = true;
                    IOUtils.write("db." + collectionName + ".find(", outputStream, StandardCharsets.UTF_8);
                } else if (SQLCommandType.DELETE.equals(sqlCommandType)) {
                    IOUtils.write("db." + collectionName + ".remove(", outputStream, StandardCharsets.UTF_8);
                } else if (SQLCommandType.UPDATE.equals(sqlCommandType)) {
                    IOUtils.write("db." + collectionName + ".updateMany(", outputStream, StandardCharsets.UTF_8);
                }
                IOUtils.write(prettyPrintJson(((Document) queryDocument.get("query")).toJson(RELAXED)),
                        outputStream, StandardCharsets.UTF_8);

                Document updateSet = (Document) queryDocument.get("updateSet");
                List<String> updateUnSet = (List<String>) queryDocument.get("updateUnSet");
                if ((updateSet != null && !updateSet.isEmpty()) || (updateUnSet != null && !updateUnSet.isEmpty())) {
                    IOUtils.write(",", outputStream, StandardCharsets.UTF_8);
                    String setString =  null;
                    String unsetString = null;
                    if (updateSet != null && !updateSet.isEmpty()) {
                        setString = prettyPrintJson(new Document().append("$set", updateSet).toJson(RELAXED));
                    }
                    if (updateUnSet != null && !updateUnSet.isEmpty()) {
                        unsetString = prettyPrintJson(new Document().append("$unset", updateUnSet).toJson(RELAXED));
                    }

                    if (setString != null && unsetString != null) {
                        IOUtils.write("[", outputStream, StandardCharsets.UTF_8);
                        IOUtils.write(setString, outputStream, StandardCharsets.UTF_8);
                        IOUtils.write(",", outputStream, StandardCharsets.UTF_8);
                        IOUtils.write(unsetString, outputStream, StandardCharsets.UTF_8);
                        IOUtils.write("]", outputStream, StandardCharsets.UTF_8);
                    } else if (setString != null) {
                        IOUtils.write(setString, outputStream, StandardCharsets.UTF_8);
                    } else if (unsetString != null) {
                        IOUtils.write(unsetString, outputStream, StandardCharsets.UTF_8);
                    }

                }

                if (queryDocument.get("projection") != null) {
                    IOUtils.write(" , ", outputStream, StandardCharsets.UTF_8);
                    IOUtils.write(prettyPrintJson(((Document) queryDocument.get("projection")).toJson(RELAXED)),
                            outputStream, StandardCharsets.UTF_8);
                }
            }
        }
        IOUtils.write(")", outputStream, StandardCharsets.UTF_8);

        if (isFindQuery) {
            if (queryDocument.get("sort") != null) {
                IOUtils.write(".sort(", outputStream, StandardCharsets.UTF_8);
                IOUtils.write(prettyPrintJson(((Document) queryDocument.get("sort")).toJson(RELAXED)),
                        outputStream, StandardCharsets.UTF_8);
                IOUtils.write(")", outputStream, StandardCharsets.UTF_8);
            }

            if (queryDocument.get("skip") != null) {
                IOUtils.write(".skip(", outputStream, StandardCharsets.UTF_8);
                IOUtils.write(queryDocument.get("skip") + "", outputStream, StandardCharsets.UTF_8);
                IOUtils.write(")", outputStream, StandardCharsets.UTF_8);
            }

            if (queryDocument.get("limit") != null) {
                IOUtils.write(".limit(", outputStream, StandardCharsets.UTF_8);
                IOUtils.write(queryDocument.get("limit") + "", outputStream, StandardCharsets.UTF_8);
                IOUtils.write(")", outputStream, StandardCharsets.UTF_8);
            }
        }
    }


    /**
     * get this query with supporting data in a document format.
     * The document has the following fields:
     * <pre>
     *     {
     *   "collection": "the collection the query is running on",
     *   "query": "the query (Document) for aggregation (List) needed to run this query",
     *   "commandType": "SELECT or DELETE",
     *   "countAll": "true if this is a count all Query",
     *   "distinct": "the field to do a distnct query on",
     *   "options": "A Document with the options for this aggregation",
     *   "projection": "The projection to use for this query"
     * }
     * </pre>
     *
     *
     * For example:
     * <pre>
     *     {
     *   "collection": "Restaurants",
     *   "query": [
     *     {
     *       "$match": {
     *         "$expr": {
     *           "$eq": [
     *             {
     *               "$toObjectId": "5e97ae59c63d1b3ff8e07c74"
     *             },
     *             "$_id"
     *           ]
     *         }
     *       }
     *     },
     *     {
     *       "$project": {
     *         "_id": 0,
     *         "id": "$_id",
     *         "R": "$Restaurant"
     *       }
     *     }
     *   ]
     * }
     * </pre>
     * @return the document object.
     */
    public Document getQueryAsDocument() {
        Document retValDocument = new Document();
        MongoDBQueryHolder mongoDBQueryHolder = getMongoQuery();
        boolean isFindQuery = false;
        final String collectionName = mongoDBQueryHolder.getCollection();
        if (mongoDBQueryHolder.isDistinct()) {
            retValDocument.put("collection", collectionName);
            retValDocument.put("distinct", getDistinctFieldName(mongoDBQueryHolder));
            retValDocument.put("query", mongoDBQueryHolder.getQuery());
        } else if (sqlCommandInfoHolder.isCountAll() && !isAggregate(mongoDBQueryHolder)) {
            retValDocument.put("countAll", true);
            retValDocument.put("collection", collectionName);
            retValDocument.put("query", mongoDBQueryHolder.getQuery());
        } else if (isAggregate(mongoDBQueryHolder)) {
            retValDocument.put("collection", collectionName);
            List<Document> aggregationDocuments = generateAggSteps(mongoDBQueryHolder, sqlCommandInfoHolder);
            retValDocument.put("query", aggregationDocuments);

            Document options = new Document();
            if (aggregationAllowDiskUse != null) {
                options.put("allowDiskUse", aggregationAllowDiskUse);
            }

            if (aggregationBatchSize != null) {
                options.put("cursor", new Document("batchSize", aggregationBatchSize));
            }

            if (options.size() > 0) {
                retValDocument.put("options", options);
            }


        } else {
            retValDocument.put("commandType", sqlCommandInfoHolder.getSqlCommandType().name());
            if (sqlCommandInfoHolder.getSqlCommandType() == SQLCommandType.SELECT) {
                isFindQuery = true;
                retValDocument.put("collection", collectionName);
            } else if (Arrays.asList(SQLCommandType.DELETE, SQLCommandType.UPDATE)
                    .contains(sqlCommandInfoHolder.getSqlCommandType())) {
                retValDocument.put("collection", collectionName);
            }
            if (mongoDBQueryHolder.getUpdateSet() != null) {
                retValDocument.put("updateSet", mongoDBQueryHolder.getUpdateSet());
            }
            if (mongoDBQueryHolder.getFieldsToUnset() != null) {
                retValDocument.put("updateUnSet", mongoDBQueryHolder.getFieldsToUnset());
            }
            retValDocument.put("query", mongoDBQueryHolder.getQuery());
            if (mongoDBQueryHolder.getProjection() != null && mongoDBQueryHolder.getProjection().size() > 0
                    && sqlCommandInfoHolder.getSqlCommandType() == SQLCommandType.SELECT) {
                retValDocument.put("projection", mongoDBQueryHolder.getProjection());
            }
        }

        if (isFindQuery) {
            if (mongoDBQueryHolder.getSort() != null && mongoDBQueryHolder.getSort().size() > 0) {
                retValDocument.put("sort", mongoDBQueryHolder.getSort());
            }

            if (mongoDBQueryHolder.getOffset() != -1) {
                retValDocument.put("skip", mongoDBQueryHolder.getOffset());
            }

            if (mongoDBQueryHolder.getLimit() != -1) {
                retValDocument.put("limit", mongoDBQueryHolder.getLimit());
            }
        }

        return retValDocument;
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
                return (T) Long.valueOf(mongoCollection.countDocuments(mongoDBQueryHolder.getQuery()));
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
        } else if (SQLCommandType.UPDATE.equals(mongoDBQueryHolder.getSqlCommandType())) {
            Document updateSet = mongoDBQueryHolder.getUpdateSet();
            List<String> fieldsToUnset = mongoDBQueryHolder.getFieldsToUnset();
            UpdateResult result = new EmptyUpdateResult();
            if ((updateSet != null && !updateSet.isEmpty()) && (fieldsToUnset != null && !fieldsToUnset.isEmpty())) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        Arrays.asList(new Document().append("$set", updateSet),
                                new Document().append("$unset", fieldsToUnset)));
            } else if (updateSet != null && !updateSet.isEmpty()) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        new Document().append("$set", updateSet));
            } else if (fieldsToUnset != null && !fieldsToUnset.isEmpty()) {
                result = mongoCollection.updateMany(mongoDBQueryHolder.getQuery(),
                        new Document().append("$unset", fieldsToUnset));
            }
            return (T) ((Long) result.getModifiedCount());
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


    private static String prettyPrintJson(final String json) {
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
         * @param inputStream the {@link InputStream} with the sql string
         * @return the builder
         */
        public Builder sqlInputStream(final InputStream inputStream) {
            notNull(inputStream);
            this.inputStream = inputStream;
            return this;
        }

        /**
         * set the sql string.
         * @param sql the sql string
         * @return the builder
         */
        public Builder sqlString(final String sql) {
            notNull(sql);
            this.inputStream = new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8));
            return this;
        }

        /**
         * set the column to {@link FieldType} mapping.
         * @param fieldNameToFieldTypeMapping the mapping from field name to {@link FieldType}
         * @return the builder
         */
        public Builder fieldNameToFieldTypeMapping(final Map<String, FieldType> fieldNameToFieldTypeMapping) {
            notNull(fieldNameToFieldTypeMapping);
            this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
            return this;
        }

        /**
         * set the default {@link FieldType}.
         * @param defaultFieldType the default {@link FieldType}
         * @return builder
         */
        public Builder defaultFieldType(final FieldType defaultFieldType) {
            notNull(defaultFieldType);
            this.defaultFieldType = defaultFieldType;
            return this;
        }

        /**
         * set whether or not aggregation is allowed to use disk use.
         * @param aggregationAllowDiskUse set to true to allow disk use during aggregation
         * @return the builder
         */
        public Builder aggregationAllowDiskUse(final Boolean aggregationAllowDiskUse) {
            notNull(aggregationAllowDiskUse);
            this.aggregationAllowDiskUse = aggregationAllowDiskUse;
            return this;
        }

        /**
         * set the batch size for aggregation.
         * @param aggregationBatchSize the batch size option to use for aggregation
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
         * @throws ParseException if there was a problem processing the sql
         */
        public QueryConverter build() throws ParseException {
            return new QueryConverter(inputStream, fieldNameToFieldTypeMapping,
                    defaultFieldType, aggregationAllowDiskUse, aggregationBatchSize);
        }
    }

    private static class AliasProjectionForGroupItems {
        private final Map<String, String> fieldToAliasMapping = new HashMap<>();
        private Document document = new Document();


        public AliasHolder getFieldToAliasMapping() {
            Map<String, String> inversedMap = Maps.transformValues(Multimaps.invertFrom(
                    Multimaps.forMap(fieldToAliasMapping), ArrayListMultimap.<String, String>create()).asMap(),
                    new com.google.common.base.Function<Collection<String>, String>() {
                @Override
                public @Nullable
                String apply(final Collection<String> input) {
                    return Iterables.getFirst(input, null);
                }
            });
            return new AliasHolder(fieldToAliasMapping, inversedMap);
        }

        public String putAlias(final String field, final String alias) {
            if (field != null) {
                return fieldToAliasMapping.put(field, alias);
            }
            return null;
        }

        public void putAll(final Map<? extends String, ? extends String> m) {
            fieldToAliasMapping.putAll(m);
        }

        public Document getDocument() {
            return document;
        }

        public AliasProjectionForGroupItems setDocument(final Document document) {
            this.document = document;
            return this;
        }
    }

    private static class EmptyUpdateResult extends UpdateResult {
        @Override
        public boolean wasAcknowledged() {
            return false;
        }

        @Override
        public long getMatchedCount() {
            return 0;
        }

        @Override
        public long getModifiedCount() {
            return 0;
        }

        @Override
        public BsonValue getUpsertedId() {
            return null;
        }
    }
}
