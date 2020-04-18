package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.MongoDBQueryHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.QueryConverter;
import com.github.vincentrussell.query.mongodb.sql.converter.WhereCauseProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.FromHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.from.SQLCommandInfoHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.ExpVisitorEraseAliasTableBaseBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorLetsBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorMatchLookupBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.WhereVisitorMatchAndLookupPipelineMatchBuilder;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class JoinProcessor {
	
	private static Document generateLetsFromON(FromHolder tholder, Expression onExp, String aliasTableName) {
		Document onDocument = new Document();
		onExp.accept(new OnVisitorLetsBuilder(onDocument, aliasTableName, tholder.getBaseAliasTable()));
		return onDocument;
	}
	
	private static Document generateMatchJoin(FromHolder tholder, Expression onExp, Expression wherePartialExp, String joinTableAlias) throws ParseException {
		Document matchJoinStep = new Document();
		onExp.accept(new OnVisitorMatchLookupBuilder(joinTableAlias,tholder.getBaseAliasTable()));
		WhereCauseProcessor whereCauseProcessor = new WhereCauseProcessor(FieldType.UNKNOWN,
				Collections.<String, FieldType>emptyMap());
        
		matchJoinStep.put("$match", whereCauseProcessor
                .parseExpression(new Document(), wherePartialExp != null? new AndExpression(onExp,wherePartialExp):onExp, null));
		return matchJoinStep;
	}
	
	private static List<Document> generateSubPipelineLookup(FromHolder tholder, Expression onExp, Expression wherePartialExp, String aliasTableName, List<Document> subqueryDocs) throws ParseException {
		List<Document> ldoc = subqueryDocs;
		ldoc.add(generateMatchJoin(tholder, onExp, wherePartialExp, aliasTableName));	
		return ldoc;
	}

	private static Document generateInternalLookup(FromHolder tholder, String joinTableName, String joinTableAlias, Expression onExp, Expression wherePartialExp, List<Document> subqueryDocs) throws ParseException {
		Document lookupInternal = new Document(); 
		lookupInternal.put("from", joinTableName);
		lookupInternal.put("let", generateLetsFromON(tholder, onExp, joinTableAlias));
		lookupInternal.put("pipeline", generateSubPipelineLookup(tholder, onExp, wherePartialExp, joinTableAlias, subqueryDocs));
		lookupInternal.put("as", joinTableAlias);
		
		return lookupInternal;
	}
	
	private static Document generateLookupStep(FromHolder tholder, String joinTableName, String joinTableAlias, Expression onExp, Expression mixedOnAndWhereExp, List<Document> subqueryDocs) throws ParseException {
		/**
		 * {
		 * 	"$lookup":{
		 * 		"from": "rightCollection",
		 * 		"let": {
		 * 			left collection ON fields
		 * 		},
		 * 		"pipeline": [
		 * 			{
		 * 				"$match": {
		 * 					whereClaseForOn
		 * 				}
		 * 			}
		 * 		],
		 * 		"as": ""
		 * 	}
		 * }
		 */
		Document lookup = new Document();
		lookup.put("$lookup", generateInternalLookup(tholder, joinTableName, joinTableAlias, onExp, mixedOnAndWhereExp,subqueryDocs));
		return lookup;
	}
	
	private static Document generateUnwindInternal(FromHolder tholder, String joinTableAlias, boolean isLeft) {
		Document internalUnwind = new Document();
		internalUnwind.put("path", "$" + joinTableAlias);
		internalUnwind.put("preserveNullAndEmptyArrays", isLeft);
		return internalUnwind;
	}
	
	private static Document generateUnwindStep(FromHolder tholder, String joinTableAlias, boolean isLeft) throws ParseException {
		/**
		 * {
		 * 	"$unwind":{
		 * 		"path": "fieldtounwind",
		 * 		"preserveNullAndEmptyArrays": (true for leftjoin false inner)
		 * 	}
		 * }
		 */
		Document unwind = new Document();
		unwind.put("$unwind", generateUnwindInternal(tholder, joinTableAlias, isLeft));
		return unwind;
	}
	
	private static Document generateInternalMatchAfterJoin(String baseAliasTable, Expression whereExpression) throws ParseException {
		WhereCauseProcessor whereCauseProcessor = new WhereCauseProcessor(FieldType.UNKNOWN,
				Collections.<String, FieldType>emptyMap());
        
		whereExpression.accept(new ExpVisitorEraseAliasTableBaseBuilder(baseAliasTable));
		
		return (Document) whereCauseProcessor
                .parseExpression(new Document(), whereExpression, null);
	}
	
	private static Document generateMatchAfterJoin(FromHolder tholder, Expression whereExpression) throws ParseException {
		/**
		 * {
		 * 	"$unwind":{
		 * 		"path": "fieldtounwind",
		 * 		"preserveNullAndEmptyArrays": (true for leftjoin false inner)
		 * 	}
		 * }
		 */
		Document match = new Document();
		match.put("$match", generateInternalMatchAfterJoin(tholder.getBaseAliasTable(), whereExpression));
		return match;
	}
	
	public static List<Document> toPipelineSteps(FromHolder tholder, List<Join> ljoins, Expression whereExpression) throws ParseException, net.sf.jsqlparser.parser.ParseException {
		List<Document> ldoc = new LinkedList<Document>();
		MutableBoolean haveOrExpression = new MutableBoolean();
		for(Join j : ljoins) {
			if(j.isInner() || j.isLeft()) {
				
				if(j.getRightItem() instanceof Table || j.getRightItem() instanceof SubSelect) {
					ExpressionHolder whereExpHolder;
					String joinTableAlias = j.getRightItem().getAlias().getName();
					String joinTableName = tholder.getSQLHolder(j.getRightItem()).getBaseTableName();
					
					whereExpHolder = new ExpressionHolder(null);
					
					if(whereExpression != null) {
						haveOrExpression.setValue(false);
						whereExpression.accept(new WhereVisitorMatchAndLookupPipelineMatchBuilder(joinTableAlias, whereExpHolder, haveOrExpression));
						if(!haveOrExpression.booleanValue() && whereExpHolder.getExpression() != null) {
							whereExpHolder.getExpression().accept(new ExpVisitorEraseAliasTableBaseBuilder(joinTableAlias));
						}
						else {
							whereExpHolder.setExpression(null);
						}
					}
					
					List<Document> subqueryDocs = new LinkedList<>();
					
					if(j.getRightItem() instanceof SubSelect) {
						QueryConverter qConverter = new QueryConverter();
						subqueryDocs = qConverter.fromSQLCommandInfoHolderToAggregateSteps((SQLCommandInfoHolder)tholder.getSQLHolder(j.getRightItem()));
					}
					
					ldoc.add(generateLookupStep(tholder,joinTableName,joinTableAlias,j.getOnExpression(),whereExpHolder.getExpression(),subqueryDocs));
					ldoc.add(generateUnwindStep(tholder,joinTableAlias,j.isLeft()));
				}
				else {
					throw new ParseException("From join not supported");
				}
			}
			else {
				throw new ParseException("Only inner join and left supported");
			}
			
		}
		if(haveOrExpression.booleanValue()) {//if there is some "or" we use this step for support this logic and no other match steps
			ldoc.add(generateMatchAfterJoin(tholder,whereExpression));
		}
		return ldoc;
	}

}
