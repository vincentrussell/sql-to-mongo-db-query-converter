package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.rule.MongoRule;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.junit.ClassRule;

public class MongoV3_4QueryConverterIT extends AbstractQueryConverterIT {

    @ClassRule
    public static MongoRule mongoRule = new MongoRule(Version.V3_4_15);

    @Override
    public MongoRule getMongoRule() {
        return mongoRule;
    }
}
