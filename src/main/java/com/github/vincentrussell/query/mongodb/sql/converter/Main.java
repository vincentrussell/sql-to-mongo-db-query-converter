package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private static JsonWriterSettings JSON_WRITER_SETTINGS = new JsonWriterSettings(JsonMode.STRICT, "\t", "\n");
    public static final String ENTER_SQL_TEXT = "Enter input sql:\n\n ";
    private static final String DEFAULT_MONGO_PORT = "27017";

    public static Options buildOptions() {
        Options options = new Options();


        final OptionGroup destinationOptionGroup = new OptionGroup();
        destinationOptionGroup.setRequired(false);

        destinationOptionGroup.addOption(Option.builder("d")
                .longOpt("destinationFile")
                .hasArg(true)
                .required(false)
                .desc("the destination file.  Defaults to System.out")
                .build());

        destinationOptionGroup.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg(true)
                .required(false)
                .desc("hosts and ports in the following format (host:port) default port is " + DEFAULT_MONGO_PORT)
                .build());


        final OptionGroup sourceOptionGroup = new OptionGroup();
        sourceOptionGroup.setRequired(false);

        sourceOptionGroup.addOption(Option.builder("s")
                .longOpt("sourceFile")
                .hasArg(true)
                .required(false)
                .desc("the source file.")
                .build());

        sourceOptionGroup.addOption(Option.builder("i")
                .longOpt("interactiveMode")
                .hasArg(false)
                .required(false)
                .desc("interactive mode")
                .build());

        sourceOptionGroup.addOption(Option.builder("sql")
                .longOpt("sql")
                .hasArg(true)
                .required(false)
                .desc("the sql select statement")
                .build());



        options.addOption(Option.builder("db")
                .longOpt("database")
                .hasArg(true)
                .required(false)
                .desc("mongo database")
                .build());

        options.addOption(Option.builder("a")
                .longOpt("auth database")
                .hasArg(true)
                .required(false)
                .desc("auth mongo database")
                .build());

        options.addOption(Option.builder("u")
                .longOpt("username")
                .hasArg(true)
                .required(false)
                .desc("usename")
                .build());

        options.addOption(Option.builder("p")
                .longOpt("password")
                .hasArg(true)
                .required(false)
                .desc("password")
                .build());

        options.addOption(Option.builder("b")
                .longOpt("batchSize")
                .hasArg(true)
                .required(false)
                .desc("batch size for query results")
                .build());

        options.addOptionGroup(sourceOptionGroup);
        options.addOptionGroup(destinationOptionGroup);

        return options;
    }

    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, org.apache.commons.cli.ParseException {
            Options options = buildOptions();


            CommandLineParser parser = new DefaultParser();
            HelpFormatter help = new HelpFormatter();
            help.setOptionComparator(new OptionComparator(Arrays.asList("s", "d", "i","h","db","a","u","p","b")));

            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                String source = cmd.getOptionValue("s");
                boolean interactiveMode = cmd.hasOption('i');

                String[] hosts = cmd.getOptionValues("h");
                String db = cmd.getOptionValue("db");
                String username = cmd.getOptionValue("u");
                String password = cmd.getOptionValue("p");
                String destination = cmd.getOptionValue("d");
                String authdb = cmd.getOptionValue("a");
                String sql = cmd.getOptionValue("sql");
                final int batchSize = Integer.parseInt(cmd.getOptionValue("b","50"));

                isFalse(hosts!=null && db==null,"provided option h, but missing db");
                isFalse(username!=null && (password==null || authdb==null),"provided option u, but missing p or a");

                isTrue(interactiveMode || source!=null || sql !=null,"Missing required option: s or i or sql");

                InputStream inputStream = null;
                OutputStream outputStream = null;

                try {
                    if (interactiveMode) {
                        inputStream = new TimeoutInputStream(System.in, 1, TimeUnit.SECONDS);
                        System.out.println(ENTER_SQL_TEXT);
                    } else if (sql!=null) {
                        inputStream = new ByteArrayInputStream(sql.getBytes());
                    } else {
                        File sourceFile = new File(source);
                        if (!sourceFile.exists()) {
                            throw new FileNotFoundException(source + " cannot be found");
                        }
                        inputStream = new FileInputStream(sourceFile);
                    }

                    if (destination!=null) {
                        File destinationFile = new File(destination);
                        if (destinationFile.exists()) {
                            throw new IOException(destination + " already exists");
                        }
                        outputStream = new FileOutputStream(destinationFile);
                    } else {
                        outputStream = new NonCloseableBufferedOutputStream(System.out);
                    }


                    QueryConverter queryConverter = new QueryConverter(inputStream);

                    inputStream.close();
                    if (hosts!=null) {
                        MongoClient mongoClient = null;
                        try {
                            mongoClient = getMongoClient(hosts, authdb, username, password);
                            Object result = queryConverter.run(mongoClient.getDatabase(db));

                            if (Long.class.isInstance(result) || long.class.isInstance(result)) {
                                IOUtils.write("\n\n******Query Results:*********\n\n", outputStream);
                                IOUtils.write(""+result,outputStream);
                                IOUtils.write("\n\n", outputStream);
                            } else if (QueryResultIterator.class.isInstance(result)) {
                                IOUtils.write("\n\n******Query Results:*********\n\n", outputStream);
                                QueryResultIterator<Document> iterator = (QueryResultIterator)result;

                                resultIterator:
                                for (Iterator<List<Document>> listIterator = Iterators.partition(iterator,batchSize); listIterator.hasNext();) {
                                    List<Document> documents = listIterator.next();
                                    IOUtils.write(toJson(documents)+"\n\n", outputStream);
                                    outputStream.flush();

                                    if (listIterator.hasNext()) {

                                        while(true) {
                                            String continueString;
                                            System.out.print("more results? (y/n): ");
                                            Scanner scanner = new Scanner(System.in);
                                            continueString = scanner.next();

                                            if ("n".equals(continueString.trim().toLowerCase())) {
                                                break resultIterator;
                                            } else if ("y".equals(continueString.trim().toLowerCase())) {
                                                break;
                                            }
                                        }
                                    }
                                }

                            }

                        } finally {
                            if (mongoClient!=null) {
                                mongoClient.close();
                            }
                        }

                    } else {
                        IOUtils.write("\n\n******Mongo Query:*********\n\n", outputStream);
                        queryConverter.write(outputStream);
                        IOUtils.write("\n\n", outputStream);
                    }


                } finally {
                    IOUtils.closeQuietly(inputStream);
                    IOUtils.closeQuietly(outputStream);
                }

            } catch (org.apache.commons.cli.ParseException e) {
                System.err.println(e.getMessage());
                help.printHelp(Main.class.getName(), options, true);
                throw e;
            }

            System.exit(0);
        }

    private static String toJson(List<Document> documents) throws IOException {
        StringWriter stringWriter = new StringWriter();
        IOUtils.write("[", stringWriter);
        IOUtils.write(Joiner.on(",").join(Lists.transform(documents, new com.google.common.base.Function<Document, String>() {
            @Override
            public String apply(Document document) {
                return document.toJson(JSON_WRITER_SETTINGS);
            }
        })),stringWriter);
        IOUtils.write("]", stringWriter);
        return stringWriter.toString();
    }

    private static MongoClient getMongoClient(String[] hosts, String authdb, String username, String password) {
        final Pattern hostAndPort = Pattern.compile("^(.[^:]*){1}([:]){0,1}(\\d+){0,1}$");
        List<ServerAddress> serverAddresses = Lists.transform(Arrays.asList(hosts), new Function<String, ServerAddress>() {
            @Override
            public ServerAddress apply(String string) {
                Matcher matcher = hostAndPort.matcher(string.trim());
                if (matcher.matches()) {
                    String hostname = matcher.group(1);
                    String port = matcher.group(3);
                    return new ServerAddress(hostname,port!=null ? Integer.parseInt(port) : Integer.parseInt(DEFAULT_MONGO_PORT));

                } else {
                    throw new IllegalArgumentException(string + " doesn't appear to be a hostname.");
                }
            }
        });
        if (username!=null && password!=null) {
            return new MongoClient(serverAddresses,Arrays.asList(MongoCredential.createCredential(username,authdb,password.toCharArray())));
        } else {
            return new MongoClient(serverAddresses);
        }
    }

    private static void isTrue(boolean expression, String message) throws org.apache.commons.cli.ParseException {
        if (!expression) {
            throw new org.apache.commons.cli.ParseException(message);
        }
    }

    private static void isFalse(boolean expression, String message) throws org.apache.commons.cli.ParseException {
        if (expression) {
            throw new org.apache.commons.cli.ParseException(message);
        }
    }


    private static class OptionComparator implements Comparator<Option> {
        private final List<String> orderList;

        public OptionComparator(List<String> orderList) {
            this.orderList = orderList;
        }


        @Override
        public int compare(Option o1, Option o2) {
            int index1 = orderList.indexOf(o1.getOpt());
            int index2 = orderList.indexOf(o2.getOpt());
            return index1 - index2;
        }
    }

}



