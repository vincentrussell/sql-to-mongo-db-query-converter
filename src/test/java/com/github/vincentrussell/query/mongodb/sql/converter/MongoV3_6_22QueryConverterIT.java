package com.github.vincentrussell.query.mongodb.sql.converter;

import com.github.vincentrussell.query.mongodb.sql.converter.rule.MongoRule;
import de.flapdoodle.embed.mongo.distribution.Version;
import org.junit.ClassRule;

public class MongoV3_6_22QueryConverterIT extends AbstractQueryConverterIT {

    @ClassRule
    public static MongoRule mongoRule = new MongoRule(Version.V3_6_22);

    @Override
    public MongoRule getMongoRule() {
        return mongoRule;
    }
}
