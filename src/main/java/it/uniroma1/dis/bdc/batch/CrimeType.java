package it.uniroma1.dis.bdc.batch;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * A simple example of Apache Flink batch processing engine using the Sacramento Police Department open dataset.
 * We here count the occurrences based on the offense code assigned to more than one crime.
 *
 * @author ichatz@gmail.com
 */
public final class CrimeType {

    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public static void main(final String[] args) throws Exception {

        // the filename to use as input dataset
        final String filename;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/crime.csv";
                System.err.println("No filename specified. Please run 'CrimeType " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'CrimeDistrict " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a dataset based on the csv file based only on the 2nd column containing the offense code
        final DataSet<Tuple1<String>> rawdata =
                env.readCsvFile(filename)
                        .includeFields("01")
                        .ignoreFirstLine()
                        .parseQuotedStrings('"')
                        .types(String.class);

        // group by offense code and count number of records per group
        rawdata.groupBy(0).reduceGroup(new CrimeCounter())
                // print the result
                .print();
    }

    /**
     * Simple class to reduce the input line (from the crime data set) into tuples (offense code, count).
     * Remark that since the input file includes numbers within double-quotes we are loading the data using a String
     * data type and then converting it to Integer.
     */
    private final static class CrimeCounter
            implements GroupReduceFunction<Tuple1<String>, Tuple2<Integer, Integer>> {

        /**
         * The method that defines the way the tupples are reduced.
         *
         * @param records - the records corresponding to this key.
         * @param out     - the tuple with the simple count of elements.
         * @throws Exception - in case any exception occurs during the conversion of the offense code into Integer.
         */
        public void reduce(Iterable<Tuple1<String>> records, Collector<Tuple2<Integer, Integer>> out) throws Exception {

            String offense = null;
            int cnt = 0;

            // iterate through the set of tuples
            for (Tuple1<String> m : records) {

                offense = m.f0;

                // count the number of occurrences
                cnt++;
            }

            // emit offense code and count
            out.collect(new Tuple2<Integer, Integer>(Integer.parseInt(offense), cnt));
        }
    }

}
