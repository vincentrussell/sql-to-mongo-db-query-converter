package com.github.vincentrussell.query.mongodb.sql.converter;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {

    public static final String ENTER_SQL_TEXT = "Enter input sql:\n\n ";

    public static Options buildOptions() {
        Options options = new Options();

        Option o = new Option("s", "sourceFile", true, "the source file.");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("d", "destinationFile", true, "the destination file.  Defaults to System.out");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("i", "interactiveMode", false, "interactive mode");
        o.setRequired(false);
        options.addOption(o);

        return options;
    }

    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, org.apache.commons.cli.ParseException {
            Options options = buildOptions();


            CommandLineParser parser = new DefaultParser();
            HelpFormatter help = new HelpFormatter();
            help.setOptionComparator(new OptionComparator(Arrays.asList("s", "d", "i")));

            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                String source = cmd.getOptionValue("s");
                boolean interactiveMode = cmd.hasOption('i');

                if (interactiveMode) {
                    System.out.println(ENTER_SQL_TEXT);
                    try (InputStream inputStream = new TimeoutInputStream(System.in, 1, TimeUnit.SECONDS);
                         OutputStream outputStream = new NonCloseableBufferedOutputStream(System.out)) {
                        IOUtils.write("\n\n******Result:*********\n\n", outputStream);
                        QueryConverter queryConverter = new QueryConverter(inputStream);
                        queryConverter.write(outputStream);

                        IOUtils.write("\n\n",outputStream);

                    }

                    System.exit(0);
                }

                String destination = cmd.getOptionValue("d");
                if (source == null) {
                    throw new org.apache.commons.cli.ParseException("Missing required option: s or i");
                }

                File sourceFile = new File(source);
                File destinationFile = destination != null ? new File(destination) : null;

                if (!sourceFile.exists()) {
                    throw new FileNotFoundException(source + " cannot be found");
                }

                if (destination != null && destinationFile.exists()) {
                    throw new IOException(destination + " already exists");
                }

                try (InputStream inputStream = new FileInputStream(sourceFile);
                     OutputStream outputStream = destinationFile != null
                             ? new FileOutputStream(destinationFile) : new NonCloseableBufferedOutputStream(System.out)) {
                    QueryConverter queryConverter = new QueryConverter(inputStream);
                    queryConverter.write(outputStream);
                }

            } catch (org.apache.commons.cli.ParseException e) {
                System.err.println(e.getMessage());
                help.printHelp(Main.class.getName(), options, true);
                throw e;
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



