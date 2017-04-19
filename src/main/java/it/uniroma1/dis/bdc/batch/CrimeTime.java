package it.uniroma1.dis.bdc.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * find out Distribution of number of crimes happened per day and sort them in descending order for each month.
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
        final DataSet< Tuple1 < String >> rawdata = env.readCsvFile(filename)
                .includeFields("1000000")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .types(String.class);

        rawdata.map(new DateExtractor()) //map data according to MM/dd/YYYY

                .groupBy(0) //group according to date
                .sum(2)  //sum the count on the field 2
                .groupBy(1)//  group the data according to field 1 that is month/year

                .sortGroup(2, Order.DESCENDING).first(50) //  arrange the data in decreasing  order

                .writeAsCsv("/home/ichatz/Downloads/CrimeTime.csv");

        env.execute();

    }

    private final static class DateExtractor
            implements MapFunction< Tuple1 < String > , Tuple3 < String, String, Integer >> {

        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat formatter2 = new SimpleDateFormat("MM/dd/yyyy HH:mm");

        public Tuple3< String, String, Integer > map(Tuple1< String > time) throws Exception {

            Date date = formatter2.parse(time.f0);

            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            int month = cal.get(Calendar.MONTH) + 1;
            int year = cal.get(Calendar.YEAR);

            return new Tuple3 <String, String, Integer > (formatter.format(date), "" + month + "/" + year, 1);
        }
    }

}
