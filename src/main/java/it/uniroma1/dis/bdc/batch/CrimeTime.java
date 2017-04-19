package it.uniroma1.dis.bdc.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A simple example of Apache Flink batch processing engine using the Sacramento Police Department open dataset.
 * find out Distribution of number of crimes happened per day.
 *
 * @author ichatz@gmail.com
 */
public class CrimeTime {

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
                System.err.println("No filename specified. Please run 'CrimeTime " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'CrimeTime " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a dataset based on the csv file
        final DataSet<Tuple2<Date, Integer>> rawdata = env.readCsvFile(filename)
                .includeFields("000000001")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class)
                .map(new MapFunction<Tuple1<String>, Tuple2<Date, Integer>>() {

                         public Tuple2<Date, Integer> map(Tuple1<String> value) throws Exception {
                             final SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

                             return new Tuple2<Date, Integer>(formatter.parse(value.f0), 1);
                         }
                     }
                );

        // count events based on day/month
        rawdata.groupBy(0) //group according to date
                .sum(1)  //sum the count
                .print();

    }

}
