package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.FromHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.SQLCommandInfoHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.ExpVisitorEraseAliasTableBaseBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorLetsBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorMatchLookupBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.WhereVisitorMatchAndLookupPipelineMatchBuilder;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Class used to help with sql joins.
 */
public final class JoinProcessor {

    private JoinProcessor() {

    }

    private static Document generateLetsFromON(final FromHolder tholder,
                                               final Expression onExp, final String aliasTableName) {
        Document onDocument = new Document();
        onExp.accept(new OnVisitorLetsBuilder(onDocument, aliasTableName, tholder.getBaseAliasTable()));
        return onDocument;
    }

    private static Document generateMatchJoin(final FromHolder tholder, final Expression onExp,
                                              final Expression wherePartialExp,
                                              final String joinTableAlias) throws ParseException {
        Document matchJoinStep = new Document();
        onExp.accept(new OnVisitorMatchLookupBuilder(joinTableAlias, tholder.getBaseAliasTable()));
        WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(FieldType.UNKNOWN,
                Collections.<String, FieldType>emptyMap(), true);

        matchJoinStep.put("$match", whereClauseProcessor
                .parseExpression(new Document(), wherePartialExp != null
                        ? new AndExpression(onExp, wherePartialExp) : onExp, null));
        return matchJoinStep;
    }

    private static List<Document> generateSubPipelineLookup(final FromHolder tholder, final Expression onExp,
                                                            final Expression wherePartialExp,
                                                            final String aliasTableName,
                                                            final List<Document> subqueryDocs) throws ParseException {
        List<Document> ldoc = subqueryDocs;
        ldoc.add(generateMatchJoin(tholder, onExp, wherePartialExp, aliasTableName));
        return ldoc;
    }

    private static Document generateInternalLookup(final FromHolder tholder, final String joinTableName,
                                                   final String joinTableAlias, final Expression onExp,
                                                   final Expression wherePartialExp,
                                                   final List<Document> subqueryDocs) throws ParseException {
        Document lookupInternal = new Document();
        lookupInternal.put("from", joinTableName);
        lookupInternal.put("let", generateLetsFromON(tholder, onExp, joinTableAlias));
        lookupInternal.put("pipeline", generateSubPipelineLookup(tholder, onExp,
                wherePartialExp, joinTableAlias, subqueryDocs));
        lookupInternal.put("as", joinTableAlias);

        return lookupInternal;
    }

    /**
     * Will perform an lookup step. Like this:
     * <pre>
     *      {
     *                "$lookup":{
     *                    "from": "rightCollection",
     *                    "let": {
     *                     left collection ON fields
     *                 },
     *                 "pipeline": [
     *                     {
     *                      "$match": {
     *                           whereClaseForOn
     *                       }
     *                     }
     *                 ],
     *                 "as": ""
     *                 }
     *               }
     * </pre>
     * @param tholder the {@link FromHolder}
     * @param joinTableName the join table name
     * @param joinTableAlias the alias for the join table
     * @param onExp {@link Expression}
     * @param mixedOnAndWhereExp the mixed on and where {@link Expression}
     * @param subqueryDocs the sub query {@link Document}s
     * @return the lookup step
     * @throws ParseException if there is an issue parsing the sql
     */
    private static Document generateLookupStep(final FromHolder tholder, final String joinTableName,
                                               final String joinTableAlias, final Expression onExp,
                                               final Expression mixedOnAndWhereExp,
                                               final List<Document> subqueryDocs) throws ParseException {
        Document lookup = new Document();
        lookup.put("$lookup", generateInternalLookup(tholder, joinTableName,
                joinTableAlias, onExp, mixedOnAndWhereExp, subqueryDocs));
        return lookup;
    }

    private static Document generateUnwindInternal(final FromHolder tholder,
                                                   final String joinTableAlias, final boolean isLeft) {
        Document internalUnwind = new Document();
        internalUnwind.put("path", "$" + joinTableAlias);
        internalUnwind.put("preserveNullAndEmptyArrays", isLeft);
        return internalUnwind;
    }

    /**
     * Will create an unwind step.  Like this:
     * <pre>
     *        {
     *                "$unwind":{
     *                "path": "fieldtounwind",
     *                "preserveNullAndEmptyArrays": (true for leftjoin false inner)
     *             }
     *          }
     * </pre>
     * @param tholder the {@link FromHolder}
     * @param joinTableAlias the alias of the join table
     * @param isLeft true if is a left join
     * @return the unwind step
     * @throws ParseException if there is an issue parsing the sql
     */
    private static Document generateUnwindStep(final FromHolder tholder,
                                               final String joinTableAlias,
                                               final boolean isLeft) throws ParseException {
        Document unwind = new Document();
        unwind.put("$unwind", generateUnwindInternal(tholder, joinTableAlias, isLeft));
        return unwind;
    }

    private static Document generateInternalMatchAfterJoin(final String baseAliasTable,
                                                           final Expression whereExpression) throws ParseException {
        WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(FieldType.UNKNOWN,
                Collections.<String, FieldType>emptyMap());

        whereExpression.accept(new ExpVisitorEraseAliasTableBaseBuilder(baseAliasTable));

        return (Document) whereClauseProcessor
                .parseExpression(new Document(), whereExpression, null);
    }

    /**
     * Will generate unwind step.  Like this:
     * <pre>
     *  {
     *               "$unwind":{
     *                  "path": "fieldtounwind",
     *                  "preserveNullAndEmptyArrays": (true for leftjoin false inner)
     *              }
     *          }
     * </pre>
     * @param tholder the {@link FromHolder}
     * @param whereExpression the where expression from the query
     * @return the unwind step
     * @throws ParseException if there is an issue parsing the sql
     */
    private static Document generateMatchAfterJoin(final FromHolder tholder,
                                                   final Expression whereExpression) throws ParseException {
        Document match = new Document();
        match.put("$match", generateInternalMatchAfterJoin(tholder.getBaseAliasTable(), whereExpression));
        return match;
    }

    /**
     *  Create the aggregation pipeline steps needed to perform a join.
     * @param queryConverter the {@link QueryConverter}
     * @param tholder the {@link FromHolder}
     * @param ljoins the list of joined tables
     * @param whereExpression the where expression from the query
     * @return the aggregation pipeline steps
     * @throws ParseException if there is an issue parsing the sql
     * @throws net.sf.jsqlparser.parser.ParseException if there is an issue parsing the sql
     */
    public static List<Document> toPipelineSteps(final QueryConverter queryConverter,
                                                 final FromHolder tholder, final List<Join> ljoins,
                                                 final Expression whereExpression)
            throws ParseException, net.sf.jsqlparser.parser.ParseException {
        List<Document> ldoc = new LinkedList<Document>();
        MutableBoolean haveOrExpression = new MutableBoolean();
        for (Join j : ljoins) {
            if (j.isInner() || j.isLeft()) {

                if (j.getRightItem() instanceof Table || j.getRightItem() instanceof SubSelect) {
                    ExpressionHolder whereExpHolder;
                    String joinTableAlias = j.getRightItem().getAlias().getName();
                    String joinTableName = tholder.getSQLHolder(j.getRightItem()).getBaseTableName();

                    whereExpHolder = new ExpressionHolder(null);

                    if (whereExpression != null) {
                        haveOrExpression.setValue(false);
                        whereExpression.accept(new WhereVisitorMatchAndLookupPipelineMatchBuilder(joinTableAlias,
                                whereExpHolder, haveOrExpression));
                        if (!haveOrExpression.booleanValue() && whereExpHolder.getExpression() != null) {
                            whereExpHolder.getExpression().accept(
                                    new ExpVisitorEraseAliasTableBaseBuilder(joinTableAlias));
                        } else {
                            whereExpHolder.setExpression(null);
                        }
                    }

                    List<Document> subqueryDocs = new LinkedList<>();

                    if (j.getRightItem() instanceof SubSelect) {
                        subqueryDocs = queryConverter.fromSQLCommandInfoHolderToAggregateSteps(
                                (SQLCommandInfoHolder) tholder.getSQLHolder(j.getRightItem()));
                    }

                    ldoc.add(generateLookupStep(tholder, joinTableName,
                            joinTableAlias, j.getOnExpression(), whereExpHolder.getExpression(), subqueryDocs));
                    ldoc.add(generateUnwindStep(tholder, joinTableAlias, j.isLeft()));
                } else {
                    throw new ParseException("From join not supported");
                }
            } else {
                throw new ParseException("Only inner join and left supported");
            }

        }
        if (haveOrExpression.booleanValue()) {
            //if there is some "or" we use this step for support this logic and no other match steps
            ldoc.add(generateMatchAfterJoin(tholder, whereExpression));
        }
        return ldoc;
    }

}
