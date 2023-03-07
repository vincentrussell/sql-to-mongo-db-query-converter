package com.github.vincentrussell.query.mongodb.sql.converter.rule;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.commands.MongodArguments;
import de.flapdoodle.embed.mongo.commands.ServerAddress;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.Transition;
import de.flapdoodle.reverse.TransitionWalker;
import de.flapdoodle.reverse.transitions.Start;
import org.junit.rules.ExternalResource;


public class MongoRule extends ExternalResource {

    private final IFeatureAwareVersion version;
    private MongoClient mongoClient;
    private TransitionWalker.ReachedState<RunningMongodProcess> running;

    public MongoRule(IFeatureAwareVersion version) {
        this.version = version;
    }

    @Override
    protected void before() throws Throwable {
        Net net = Net.of(de.flapdoodle.net.Net.getLocalHost().getHostName(),
                de.flapdoodle.net.Net.freeServerPort(),
                de.flapdoodle.net.Net.localhostIsIPv6());

        Mongod mongod = new Mongod() {
            @Override
            public Transition<MongodArguments> mongodArguments() {
                return Start.to(MongodArguments.class)
                        .initializedWith(MongodArguments.defaults()
                                .withUseNoPrealloc(false)
                                .withUseSmallFiles(false));
            }

            @Override
            public Transition<Net> net() {
                return Start.to(Net.class).initializedWith(net);
            }
        };
        running = mongod.start(version);
        ServerAddress serverAddress = running.current().getServerAddress();
        mongoClient = MongoClients.create("mongodb://" + serverAddress.toString());
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    @Override
    protected void after() {
        mongoClient.close();
        running.close();
    }
}
