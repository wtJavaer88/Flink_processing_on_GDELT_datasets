package it.uniroma1.dis.bdc.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * A simple example of Apache Flink stream processing engine using the Sacramento Police Department open dataset.
 * We here count the number of occurrences per city district.
 * <p>
 * The stream of data is arriving from a very simple and naive socket server.
 *
 * @author ichatz@gmail.com
 * @see DataSetServer
 *
 *
 */
public class CrimeDistrict {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port") : DataSetServer.SOCKET_PORT;

        } catch (Exception e) {
            System.err.println("No port specified. Please run 'CrimeStream " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port (9999 by default) is the address of the dataset server");
            System.err.println("To start a simple dataset server, check out the DataSetServer class");
            return;
        }

        // The StreamExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<Tuple2<Integer, Integer>> rawdata = text

                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {

                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) {
                        final String[] items = value.split(",");
                        // extract district
                        final String txtDistrict = items[5];
                        try {
                            final int intDistrict = Integer.parseInt(txtDistrict.substring(1, txtDistrict.length() - 1));

                            out.collect(new Tuple2(intDistrict, 1));
                        } catch (Exception ex) {
                            // either district does not exist or it is not an integer
                            // simply ignore
                        }
                    }
                })

                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {

                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return new Tuple2(a.f0, a.f1 + b.f1);
                    }
                });

        // print the results with a single thread, rather than in parallel
        rawdata.print().setParallelism(1);

        env.execute("Socket Window CrimeStreamDistrict");
    }

}
