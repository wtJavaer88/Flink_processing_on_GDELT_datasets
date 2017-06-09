package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Tomassi Valerio 1756285.
 */
public class stateVSNonStateActors_daily {
    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public static void main(final String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> rawdata = env.readTextFile("./2005.csv");

        DataStream<Tuple3<String, String, Integer>> text = rawdata
                .flatMap(new Splitter());

        DataStream<Tuple3<String, String, Integer>> withTimestamp =
                text.assignTimestampsAndWatermarks(new CustomWatermark());


        DataStream<Tuple3<String, String, Integer>> grouped =
                withTimestamp
                        .keyBy(1)
                        .window(TumblingEventTimeWindows.of(Time.days(1L)))
                        .sum(2);

        grouped.print();


        grouped
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1L)))
                .fold(new ArrayList<Tuple3<String, String, Integer>>(), new FoldFunction<Tuple3<String, String, Integer>, ArrayList<Tuple3<String, String, Integer>>>() {

                    public ArrayList<Tuple3<String, String, Integer>> fold(ArrayList<Tuple3<String, String, Integer>> cur, Tuple3<String, String, Integer> value) {

                        cur.add(value);
                        return cur;
                    }
                })
                .print();


        env.execute();
    }

    public static class CustomWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Integer>> {

        private static final long serialVersionUID = 1L;
        private long maxTimestamp = 0;
        private long priorTimestamp = 0;
        private final long maxDelay = 86400000; //1 day
        private Calendar calendar = GregorianCalendar.getInstance();


        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(this.maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple3<String, String, Integer> tuple, long priorTimestamp) {

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Date date = null;

            try {
                date = sdf.parse(tuple.f0);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            calendar.setTime(date);
            maxTimestamp = Math.max(calendar.getTimeInMillis(), priorTimestamp);

            return calendar.getTimeInMillis();
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple3<String, String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] words = line.split("\t");
            String date = words[1];
            String actorType = words[12];
            if (!(actorType.equals("IMG") || actorType.equals("NMG") || actorType.equals("NGO") || actorType.equals("IGO"))) {
                out.collect(new Tuple3<>(date, "STATE", 1));
            } else {
                out.collect(new Tuple3<>(date, "NON-STATE", 1));
            }
        }

    }
}

