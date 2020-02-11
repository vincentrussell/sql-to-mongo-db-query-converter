package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.WhereCauseProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.TablesHolder;
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

import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class JoinProcessor {
	
	private static Document generateLetsFromON(TablesHolder tholder, Expression onExp, Table t) {
		Document onDocument = new Document();
		onExp.accept(new OnVisitorLetsBuilder(onDocument, t.getAlias().getName(), tholder.getBaseAliasTable()));
		return onDocument;
	}
	
	private static Document generateMatchJoin(TablesHolder tholder, Expression onExp, Expression wherePartialExp, Table t) throws ParseException {
		Document matchJoinStep = new Document();
		onExp.accept(new OnVisitorMatchLookupBuilder(t.getAlias().getName(),tholder.getBaseAliasTable()));
		WhereCauseProcessor whereCauseProcessor = new WhereCauseProcessor(FieldType.UNKNOWN,
				Collections.<String, FieldType>emptyMap());
        
		matchJoinStep.put("$match", whereCauseProcessor
                .parseExpression(new Document(), wherePartialExp != null? new AndExpression(onExp,wherePartialExp):onExp, null));
		return matchJoinStep;
	}
	
	private static List<Document> generateSubPipelineLookup(TablesHolder tholder, Expression onExp, Expression wherePartialExp, Table t) throws ParseException {
		List<Document> ldoc = new LinkedList<Document>();
		ldoc.add(generateMatchJoin(tholder, onExp, wherePartialExp, t));	
		return ldoc;
	}

	private static Document generateInternalLookup(TablesHolder tholder, Table t, Expression onExp, Expression wherePartialExp) throws ParseException {
		Document lookupInternal = new Document(); 
		lookupInternal.put("from", t.getName());
		lookupInternal.put("let", generateLetsFromON(tholder, onExp, t));
		lookupInternal.put("pipeline", generateSubPipelineLookup(tholder, onExp, wherePartialExp, t));
		lookupInternal.put("as", tholder.getAlias(t.getName()));
		
		return lookupInternal;
	}
	
	private static Document generateLookupStep(TablesHolder tholder, Table table, Expression onExp, Expression mixedOnAndWhereExp) throws ParseException {
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
		lookup.put("$lookup", generateInternalLookup(tholder, table, onExp, mixedOnAndWhereExp));
		return lookup;
	}
	
	private static Document generateUnwindInternal(TablesHolder tholder, Table t, boolean isLeft) {
		Document internalUnwind = new Document();
		internalUnwind.put("path", "$" + tholder.getAlias(t.getName()));
		internalUnwind.put("preserveNullAndEmptyArrays", isLeft);
		return internalUnwind;
	}
	
	private static Document generateUnwindStep(TablesHolder tholder, Table t, boolean isLeft) throws ParseException {
		/**
		 * {
		 * 	"$unwind":{
		 * 		"path": "fieldtounwind",
		 * 		"preserveNullAndEmptyArrays": (true for leftjoin false inner)
		 * 	}
		 * }
		 */
		Document unwind = new Document();
		unwind.put("$unwind", generateUnwindInternal(tholder, t, isLeft));
		return unwind;
	}
	
	private static Document generateInternalMatchAfterJoin(String baseAliasTable, Expression whereExpression) throws ParseException {
		WhereCauseProcessor whereCauseProcessor = new WhereCauseProcessor(FieldType.UNKNOWN,
				Collections.<String, FieldType>emptyMap());
        
		whereExpression.accept(new ExpVisitorEraseAliasTableBaseBuilder(baseAliasTable));
		
		return (Document) whereCauseProcessor
                .parseExpression(new Document(), whereExpression, null);
	}
	
	private static Document generateMatchAfterJoin(TablesHolder tholder, Expression whereExpression) throws ParseException {
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
	
	public static List<Document> toPipelineSteps(TablesHolder tholder, List<Join> ljoins, Expression whereExpression) throws ParseException {
		List<Document> ldoc = new LinkedList<Document>();
		MutableBoolean haveOrExpression = new MutableBoolean();
		for(Join j : ljoins) {
			if(j.isInner() || j.isLeft()) {
				if(j.getRightItem() instanceof Table) {
					Table t = (Table)j.getRightItem();
					ExpressionHolder whereExpHolder = new ExpressionHolder(null);
					
					if(whereExpression != null) {
						haveOrExpression.setValue(false);
						whereExpression.accept(new WhereVisitorMatchAndLookupPipelineMatchBuilder(t.getAlias().getName(), whereExpHolder, haveOrExpression));
						if(!haveOrExpression.booleanValue() && whereExpHolder.getExpression() != null) {
							whereExpHolder.getExpression().accept(new ExpVisitorEraseAliasTableBaseBuilder(t.getAlias().getName()));
						}
						else {
							whereExpHolder.setExpression(null);
						}
					}
					ldoc.add(generateLookupStep(tholder,t,j.getOnExpression(),whereExpHolder.getExpression()));
					ldoc.add(generateUnwindStep(tholder,t,j.isLeft()));
				}
				else {//Subselect...
					throw new ParseException("Only join with table no subqueries");
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
