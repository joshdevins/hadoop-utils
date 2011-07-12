package net.joshdevins.hadoop.utils;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Simple utilities for drivers and tools.
 * 
 * @author Josh Devins
 */
public final class MainUtils {

    private MainUtils() {
        throw new UnsupportedOperationException();
    }

    public static void exitWithError(final String message) {
        System.err.println(message);
        throw new ExitException(message);
    }

    public static void exitWithStackTraceAndError(final String message, final Exception e) {
        printStackTraceAndError(message, e);
        throw new ExitException(message, e);
    }

    public static void printStackTraceAndError(final String message, final Exception e) {
        e.printStackTrace();
        System.err.println(message);
    }

    public static void toolRunner(final Tool tool, final String[] args) throws Exception {

        try {
            System.exit(toolRunnerWithoutExit(tool, args));
        } catch (ExitException ee) {
            System.exit(-1);
        }
    }

    public static int toolRunnerWithoutExit(final Tool tool, final String[] args) throws Exception {
        return ToolRunner.run(new Configuration(), tool, args);
    }

    public static void validateStandardInputOutputDriver(final Class<?> clazz, final String[] args) {

        try {
            Validate.notNull(args);
            Validate.isTrue(args.length == 2);

            Validate.notEmpty(args[0]);
            Validate.notEmpty(args[1]);

        } catch (IllegalArgumentException iae) {

            System.err.printf("Usage: %s [generic options] <input> <output>\n", clazz.getSimpleName()); // NOPMD
            GenericOptionsParser.printGenericCommandUsage(System.err);
            throw new ExitException("Illegal argument expcetion");
        }
    }
}
