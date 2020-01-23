package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;
import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.WhereCauseProcessor;
import com.github.vincentrussell.query.mongodb.sql.converter.holder.TablesHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorLetsBuilder;
import com.github.vincentrussell.query.mongodb.sql.converter.visitor.OnVisitorMatchLookupBuilder;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

import org.bson.Document;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class JoinProcessor {
	
	private static Document generateLetsFromON(TablesHolder tholder, Expression onExp, Table t) {
		Document onDocument = new Document();
		onExp.accept(new OnVisitorLetsBuilder(onDocument, t.getAlias().getName()));
		return onDocument;
	}
	
	private static Document generateMatchJoin(TablesHolder tholder, Expression onExp, Table t) throws ParseException {
		Document matchJoinStep = new Document();
		onExp.accept(new OnVisitorMatchLookupBuilder(t.getAlias().getName()));
		WhereCauseProcessor whereCauseProcessor = new WhereCauseProcessor(FieldType.UNKNOWN,
				Collections.<String, FieldType>emptyMap());
        
		matchJoinStep.put("$match", whereCauseProcessor
                .parseExpression(new Document(), onExp, null));
		return matchJoinStep;
	}
	
	private static List<Document> generateSubPipelineLookup(TablesHolder tholder, Expression onExp, Table t) throws ParseException {
		List<Document> ldoc = new LinkedList<Document>();
		ldoc.add(generateMatchJoin(tholder, onExp, t));	
		return ldoc;
	}

	private static Document generateInternalLookup(TablesHolder tholder, Table t, Expression onExp) throws ParseException {
		Document lookupInternal = new Document(); 
		lookupInternal.put("from", t.getName());
		lookupInternal.put("let", generateLetsFromON(tholder, onExp, t));
		lookupInternal.put("pipeline", generateSubPipelineLookup(tholder, onExp, t));
		lookupInternal.put("as", tholder.getAlias(t.getName()));
		
		return lookupInternal;
	}
	
	private static Document generateLookupStep(TablesHolder tholder, Table table, Expression onExp) throws ParseException {
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
		lookup.put("$lookup", generateInternalLookup(tholder, table, onExp));
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
	
	public static List<Document> toPipelineSteps(TablesHolder tholder, List<Join> ljoins) throws ParseException {
		List<Document> ldoc = new LinkedList<Document>();
		for(Join j : ljoins) {
			if(j.isInner() || j.isLeft()) {
				if(j.getRightItem() instanceof Table) {
					Table t = (Table)j.getRightItem();
					Expression onExp = j.getOnExpression();
					
					ldoc.add(generateLookupStep(tholder,t,onExp));
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
		return ldoc;
	}

}
