package it.uniroma1.dis.bdc.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Streaming version.
 */
public class CrimeStream {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
                    "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<DistrictWithCount> rawdata = text

                .flatMap(new FlatMapFunction<String, DistrictWithCount>() {

                    public void flatMap(String value, Collector<DistrictWithCount> out) {
                        String[] items = value.split(",");
                        if (items.length > 6) {
                            out.collect(new DistrictWithCount(items[6], 1L));
                        }
                    }
                })

                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<DistrictWithCount>() {

                    public DistrictWithCount reduce(DistrictWithCount a, DistrictWithCount b) {
                        return new DistrictWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        rawdata.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    /**
     * Data type for words with count
     */
    public static class DistrictWithCount {

        public String word;
        public long count;

        public DistrictWithCount() {}

        public DistrictWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

}
