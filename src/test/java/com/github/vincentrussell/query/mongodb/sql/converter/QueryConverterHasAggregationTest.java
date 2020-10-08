package com.github.vincentrussell.query.mongodb.sql.converter;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryConverterHasAggregationTest {

    QueryConverter.Builder builder;

    @Before
    public void before() {
        builder = new QueryConverter.Builder();
    }

    @Test
    public void simpleEquals() throws ParseException {
        QueryConverter queryConverter = builder.sqlString("select * from my_table WHERE column = \"value\"").build();
        assertFalse(queryConverter.getMongoQuery().isRequiresMultistepAggregation());
    }

    @Test
    public void subQuery() throws ParseException {
        QueryConverter queryConverter = builder.sqlString("select c.borough from(select borough, cuisine from Restautants limit 2) as c limit 1").build();
        assertTrue(queryConverter.getMongoQuery().isRequiresMultistepAggregation());
    }

    @Test
    public void groupBy() throws ParseException {
        QueryConverter queryConverter = builder.sqlString("select c.cuisine, c.c as c  from(select borough, cuisine, count(*) as c from Restaurants group by borough, cuisine limit 6000) as c where c.cuisine = 'Hamburgers' and c.borough ='Manhattan'").build();
        assertTrue(queryConverter.getMongoQuery().isRequiresMultistepAggregation());
    }

    @Test
    public void innerJoin() throws ParseException {
        QueryConverter queryConverter = builder.sqlString("select t1.column1, t2.column2 from my_table as t1 inner join my_table2 as t2 on t1.column = t2.column and t2.column2 = t1.column2").build();
        assertTrue(queryConverter.getMongoQuery().isRequiresMultistepAggregation());
    }
}
