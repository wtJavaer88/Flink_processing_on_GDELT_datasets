package it.uniroma1.dis.bdc.batch;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * find out the Hour of the day when maximum crime occurs (Highest Crime Hour Analysis) for each day.
 */
public class CrimeMaxHour {

    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public final static void main(final String[] args) throws Exception {

        // the filename to use as input dataset
        final String filename;
        try {
            // access the arguments of the command line tool
            final ParameterTool params = ParameterTool.fromArgs(args);
            if (!params.has("filename")) {
                filename = "/tmp/crime.csv";
                System.err.println("No filename specified. Please run 'CrimeDistrict " +
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

        // create a dataset based on the csv file
        final DataSet<Tuple1< String >> rawdata = env.readCsvFile(filename)
                .includeFields("1000000")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class);

        rawdata.map(new TimeExtractor()) //map the data according to the MM/dd/yyyy HH
                .groupBy(0, 1) //group the data according to date & hour
                .sum(2) // sum on the field 2 to count
                .groupBy(0) //group the data according to field date
                .maxBy(0,2) //to find out the maximum on the basis of per day

                .print();  //print the result

    }

    private final static class TimeExtractor
            implements MapFunction< Tuple1 < String >, Tuple3<String, String, Integer>> {

        public Tuple3<String, String, Integer> map(Tuple1<String> time) throws Exception {
            SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH");
            SimpleDateFormat formatter2 = new SimpleDateFormat("MM/dd/yyyy HH:mm");

            String dateInString = time.f0;
            Date date = formatter2.parse(dateInString);

            String dateTokens[] = formatter.format(date).split(" ");

            return new Tuple3<String, String, Integer>(dateTokens[0], dateTokens[1], 1);
        }
    }

}
