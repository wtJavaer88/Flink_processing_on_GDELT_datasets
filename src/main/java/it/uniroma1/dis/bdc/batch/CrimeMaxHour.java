package it.uniroma1.dis.bdc.batch;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * A simple example of Apache Flink batch processing engine using the Sacramento Police Department open dataset.
 * find out the Hour of the day when maximum crime occurs (Highest Crime Hour Analysis) for each day.
 *
 * @author ichatz@gmail.com
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
                System.err.println("No filename specified. Please run 'CrimeMaxHour " +
                        "--filename <filename>, where filename is the name of the dataset in CSV format");
            } else {
                filename = params.get("filename");
            }

        } catch (Exception ex) {
            System.err.println("No filename specified. Please run 'CrimeMaxHour " +
                    "--filename <filename>, where filename is the name of the dataset in CSV format");
            return;
        }

        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create a dataset based on the csv file
        env.readCsvFile(filename)
                .includeFields("0000000011")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class, String.class)
                .map(new TimeExtractor()) //map the data according to the MM/dd/yyyy 00:HH:mm
                .groupBy(0, 1) //group the data according to date & hour
                .sum(2) // sum on the field 2 to count
                .groupBy(0) //group the data according to field date
                .maxBy(0, 2) //to find out the maximum on the basis of per day
                .print();  //print the result
    }

    /**
     * Translate line (from the crime data set) into 3-value tuples based on the date and the time of the crime.
     */
    private final static class TimeExtractor
            implements MapFunction<Tuple2<String, String>, Tuple3<Date, Integer, Integer>> {

        public Tuple3<Date, Integer, Integer> map(Tuple2<String, String> input) throws Exception {
            final SimpleDateFormat formatterDate = new SimpleDateFormat("MM/dd/yyyy");
            final SimpleDateFormat formatterTime = new SimpleDateFormat("HH:mm");

            final Date date = formatterDate.parse(input.f0);
            final Date time = formatterTime.parse(input.f1.substring(3));

            final Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
            calendar.setTime(time);   // assigns calendar to given date
            final int hour = calendar.get(Calendar.HOUR);

            return new Tuple3<Date, Integer, Integer>(date, hour, 1);
        }
    }

}
