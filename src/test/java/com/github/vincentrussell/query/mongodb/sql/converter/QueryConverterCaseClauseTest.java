package com.github.vincentrussell.query.mongodb.sql.converter;

import org.junit.Test;

import java.io.ByteArrayOutputStream;

/**
 * case when expr then true-part else false-part end => $cond test .
 *
 * @author maxid
 * @since 2024/8/22 14:45
 */
public class QueryConverterCaseClauseTest {
    @Test
    public void testCaseExpression() throws Exception {
        String sql = "select n.module, " +
                "sum(case when n.url != null then 1 else 0 end) as nullRest, " +
                "sum(case when n.url not like '%user%' then 1 else 0 end) as userRest, " +
//                "sum(case when n.url not like '%user%' then 1 else 0 end) as userRest, " +
//                "sum(case when n.module in('数据字典管理') then 1 else 0 end) as dictRest, " +
                "sum(case when n.cost<=10 then 1 else 0 end) as lte10, " +
                "sum(case when n.cost>10 and n.cost<=40 then 1 else 0 end) as gt10lte40 " +
                "from saas_audits n " +
                "where n.method like '%etoneiot%' " +
//                "and n.url != null " +
//                "and n.url is null " +
//                "and n.url is not null " +
//                "and n.cost between 10 and 40 " +
                "group by n.module";
        QueryConverter queryConverter = new QueryConverter.Builder().sqlString(sql).build();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        queryConverter.write(byteArrayOutputStream);
        System.out.println(byteArrayOutputStream);
    }
}
