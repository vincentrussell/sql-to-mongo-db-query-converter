package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Main {


    public static final int DEFAULT_RESULT_BATCH_SIZE = 50;
    private static final JsonWriterSettings JSON_WRITER_SETTINGS = JsonWriterSettings.builder()
            .outputMode(JsonMode.RELAXED).indentCharacters("\t").newLineCharacters("\n").build();
    public static final String ENTER_SQL_TEXT = "Enter input sql:\n\n ";
    public static final String CONTINUE_TEXT = "Would you like to continue? (y/n):\n\n ";
    private static final String DEFAULT_MONGO_PORT = "27017";
    public static final String D_AGGREGATION_ALLOW_DISK_USE = "aggregationAllowDiskUse";
    public static final String D_AGGREGATION_BATCH_SIZE = "aggregationBatchSize";

    private Main() {

    }

    private static Options buildOptions() {
        Options options = new Options();


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


        options.addOption(Option.builder("d")
                .longOpt("destinationFile")
                .hasArg(true)
                .required(false)
                .desc("the destination file.  Defaults to System.out")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("host")
                .hasArg(true)
                .required(false)
                .desc("hosts and ports in the following format (host:port) default port is " + DEFAULT_MONGO_PORT)
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

        options.addOption(Option.builder("l")
                .longOpt("loopMode")
                .hasArg(false)
                .required(false)
                .desc("interactive loopMode mode")
                .build());

        options.addOptionGroup(sourceOptionGroup);

        return options;
    }

    /**
     * Main method.
     * @param args the args for the program
     * @throws IOException if there is problem reading the sql file or writing to an output file
     * @throws ParseException if there is a problem parsing the sql
     * @throws org.apache.commons.cli.ParseException if there is a problem processing the args.
     */
    public static void main(final String[] args) throws IOException,
            ParseException, org.apache.commons.cli.ParseException {

        Options options = buildOptions();

        CommandLineParser parser = new DefaultParser();
        HelpFormatter help = new HelpFormatter();
        help.setOptionComparator(new OptionComparator(
                Arrays.asList("s", "sql", "i", "l", "d", "h", "db", "a", "u", "p", "b")));

        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            String[] hosts = cmd.getOptionValues("h");
            final boolean loopMode = cmd.hasOption('l');

            verifyArguments(cmd);

            while (true) {
                try (InputStream inputStream = getInputStream(cmd);
                     OutputStream outputStream = getOutputStream(cmd)) {

                    QueryConverter queryConverter = getQueryConverter(inputStream);

                    if (hosts != null) {
                        try {
                            runQueryInMongo(cmd, hosts, outputStream, queryConverter);
                        } catch (ParseException | IOException e) {
                            if (loopMode) {
                                e.printStackTrace(System.err);
                                continue;
                            } else {
                                throw e;
                            }
                        }
                    } else {
                        IOUtils.write("\n\n******Mongo Query:*********\n\n", outputStream);
                        queryConverter.write(outputStream);
                        IOUtils.write("\n\n", outputStream);
                    }

                }

                if (loopMode) {
                    if (shouldContinue()) {
                        continue;
                    }
                }
                break;
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println(e.getMessage());
            help.printHelp(Main.class.getName(), options, true);
            throw e;
        }

        System.exit(0);
    }

    private static boolean shouldContinue() throws IOException {
        if ("y".equals(getCharacterInput(CONTINUE_TEXT).trim().toLowerCase())) {
            return true;
        }
            return false;
    }

    private static void verifyArguments(final CommandLine cmd) throws org.apache.commons.cli.ParseException {
        final String source = cmd.getOptionValue("s");
        final boolean interactiveMode = cmd.hasOption('i');
        final String[] hosts = cmd.getOptionValues("h");
        final String sql = cmd.getOptionValue("sql");
        final String db = cmd.getOptionValue("db");
        final String username = cmd.getOptionValue("u");
        final String password = cmd.getOptionValue("p");
        final String authdb = cmd.getOptionValue("a");

        isTrue(interactiveMode || source != null || sql != null,
                "Missing required option: s or i or sql");
        isFalse(hosts != null && db == null,
                "provided option h, but missing db");
        isFalse(username != null && (password == null || authdb == null),
                "provided option u, but missing p or a");
    }

    private static void runQueryInMongo(final CommandLine cmd, final String[] hosts, final OutputStream outputStream,
                                        final QueryConverter queryConverter) throws ParseException, IOException {

        final String db = cmd.getOptionValue("db");
        final String username = cmd.getOptionValue("u");
        final String password = cmd.getOptionValue("p");
        final String authdb = cmd.getOptionValue("a");
        final int batchSize = Integer.parseInt(cmd.getOptionValue("b", "" + DEFAULT_RESULT_BATCH_SIZE));

        MongoClient mongoClient = null;
        try {
            mongoClient = getMongoClient(hosts, authdb, username, password);
            Object result = queryConverter.run(mongoClient.getDatabase(db));

            if (Long.class.isInstance(result) || long.class.isInstance(result)) {
                IOUtils.write("\n\n******Query Results:*********\n\n", outputStream);
                IOUtils.write("" + result, outputStream);
                IOUtils.write("\n\n", outputStream);
            } else if (QueryResultIterator.class.isInstance(result)) {
                processMongoResults(batchSize, outputStream, (QueryResultIterator) result);
            }

        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private static QueryConverter getQueryConverter(final InputStream inputStream) throws ParseException {

        QueryConverter.Builder builder = new QueryConverter.Builder().sqlInputStream(inputStream);

        if (System.getProperty(D_AGGREGATION_ALLOW_DISK_USE) != null) {
            builder.aggregationAllowDiskUse(Boolean.valueOf(
                    System.getProperty(D_AGGREGATION_ALLOW_DISK_USE)));
        }

        if (System.getProperty(D_AGGREGATION_BATCH_SIZE) != null) {
            try {
                builder.aggregationBatchSize(Integer.valueOf(System.getProperty(D_AGGREGATION_BATCH_SIZE)));
            } catch (NumberFormatException formatException) {
                System.err.println(formatException.getMessage());
            }
        }

        return builder.build();
    }

    private static OutputStream getOutputStream(final CommandLine cmd) throws IOException {
        final String destination = cmd.getOptionValue("d");
        OutputStream outputStream = null;
        if (destination != null) {
            File destinationFile = new File(destination);
            if (destinationFile.exists()) {
                throw new IOException(destination + " already exists");
            }
            outputStream = new FileOutputStream(destinationFile);
        } else {
            outputStream = new NonCloseableBufferedOutputStream(System.out);
        }
        return outputStream;
    }

    private static InputStream getInputStream(final CommandLine cmd) throws FileNotFoundException {
        final String source = cmd.getOptionValue("s");
        final boolean interactiveMode = cmd.hasOption('i');
        final String sql = cmd.getOptionValue("sql");

        InputStream inputStream = null;
        if (interactiveMode) {
            inputStream = new TimeoutInputStream(new UncloseableInputStream(System.in), 1, TimeUnit.SECONDS);
            System.out.println(ENTER_SQL_TEXT);
        } else if (sql != null) {
            inputStream = new ByteArrayInputStream(sql.getBytes(Charsets.UTF_8));
        } else {
            File sourceFile = new File(source);
            if (!sourceFile.exists()) {
                throw new FileNotFoundException(source + " cannot be found");
            }
            inputStream = new FileInputStream(sourceFile);
        }
        return inputStream;
    }

    private static void processMongoResults(final int batchSize, final OutputStream outputStream,
                                            final QueryResultIterator result) throws IOException {
        QueryResultIterator<Document> iterator = result;

        if (FileOutputStream.class.isInstance(outputStream)) {
            IOUtils.write("[", outputStream);
            while (iterator.hasNext()) {
                IOUtils.write(iterator.next().toJson(), outputStream);
                if (iterator.hasNext()) {
                    IOUtils.write(",\n", outputStream);
                }
            }
            IOUtils.write("]", outputStream);

        } else {
            IOUtils.write("\n\n******Query Results:*********\n\n", outputStream);

            resultIterator:
            for (Iterator<List<Document>> listIterator = Iterators
                    .partition(iterator, batchSize); listIterator.hasNext();) {
                List<Document> documents = listIterator.next();
                IOUtils.write(toJson(documents) + "\n\n", outputStream);
                outputStream.flush();

                if (listIterator.hasNext()) {

                    inputLoop:
                    while (true) {
                        String continueString;
                        continueString = getCharacterInput("more results? (y/n): ");

                        if ("n".equals(continueString.trim().toLowerCase())) {
                            break resultIterator;
                        } else if ("y".equals(continueString.trim().toLowerCase())) {
                            break inputLoop;
                        }
                    }
                }
            }
        }
    }

    private static String getCharacterInput(final String question) throws IOException {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);

        Future<String> future = executorService.submit(new Callable<String>() {
            @SuppressWarnings("checkstyle:magicnumber")
            @Override
            public String call() throws Exception {
                System.out.print(question);
                while (true) {
                    Scanner scanner = new Scanner(new UncloseableInputStream(System.in), Charsets.UTF_8.displayName());
                    String choice = "";
                    if (scanner.hasNext()) {
                        choice = scanner.next();
                    } else {
                        Thread.sleep(200L);
                        continue;
                    }
                    return choice;
                }
            }
        });

        try {
            return future.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException(e);
        }

    }

    private static String toJson(final List<Document> documents) throws IOException {
        StringWriter stringWriter = new StringWriter();
        IOUtils.write("[", stringWriter);
        IOUtils.write(Joiner.on(",").join(Lists.transform(documents,
                new com.google.common.base.Function<Document, String>() {
                    @Override
                    public String apply(@Nonnull final Document document) {
                        return document.toJson(JSON_WRITER_SETTINGS);
                    }
                })), stringWriter);
        IOUtils.write("]", stringWriter);
        return stringWriter.toString();
    }

    @SuppressWarnings("magicnumber")
    private static MongoClient getMongoClient(final String[] hosts,
                                              final String authdb, final String username, final String password) {
        final Pattern hostAndPort = Pattern.compile("^(.[^:]*){1}([:]){0,1}(\\d+){0,1}$");
        List<ServerAddress> serverAddresses = Lists.transform(Arrays.asList(hosts),
                new Function<String, ServerAddress>() {
                    @Override
                    public ServerAddress apply(@Nonnull final String string) {
                        Matcher matcher = hostAndPort.matcher(string.trim());
                        if (matcher.matches()) {
                            String hostname = matcher.group(1);
                            String port = matcher.group(3);
                            return new ServerAddress(hostname, port != null
                                    ? Integer.parseInt(port) : Integer.parseInt(DEFAULT_MONGO_PORT));

                        } else {
                            throw new IllegalArgumentException(string + " doesn't appear to be a hostname.");
                        }
                    }
                });
        if (username != null && password != null) {
            return new MongoClient(serverAddresses,
                    Arrays.asList(MongoCredential.createCredential(username, authdb, password.toCharArray())));
        } else {
            return new MongoClient(serverAddresses);
        }
    }

    private static void isTrue(final boolean expression, final String message)
            throws org.apache.commons.cli.ParseException {
        if (!expression) {
            throw new org.apache.commons.cli.ParseException(message);
        }
    }

    private static void isFalse(final boolean expression, final String message)
            throws org.apache.commons.cli.ParseException {
        if (expression) {
            throw new org.apache.commons.cli.ParseException(message);
        }
    }


    private static class OptionComparator implements Comparator<Option>, Serializable {
        private final List<String> orderList;

        OptionComparator(final List<String> orderList) {
            this.orderList = orderList;
        }


        @Override
        public int compare(final Option o1, final Option o2) {
            int index1 = orderList.indexOf(o1.getOpt());
            int index2 = orderList.indexOf(o2.getOpt());
            return index1 - index2;
        }
    }

}



