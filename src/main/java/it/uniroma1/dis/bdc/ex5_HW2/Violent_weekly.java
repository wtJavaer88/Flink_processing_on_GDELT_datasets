package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Violent_weekly {

    public static void main(final String[] args) throws Exception {


        final Calendar calendar  = Calendar.getInstance();
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

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
                        .window(TumblingEventTimeWindows.of(Time.days(7L)))
                        .apply(new WindowFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, Tuple, TimeWindow>() {
                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Integer>> iterable,
                                              Collector<Tuple3<String, String, Integer>> collector) throws Exception {

                                int count = 0;

                                String event ="";
                                String date = "";

                                for(Tuple3<String, String, Integer> t  : iterable) {

                                    event = t.f1;
                                    date = t.f0;

                                    count += t.f2;
                                }

                                Date d = sdf.parse(date);
                                calendar.setTime(d);
                                int week = calendar.get(Calendar.WEEK_OF_YEAR);

                                collector.collect(new Tuple3<>(String.valueOf(week),event,count ));
                            }
                        });
        grouped.print();



        env.execute();
    }

    public static class CustomWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Integer>>{

        private static final long serialVersionUID = 1L;
        private long maxTimestamp = 0;
        private long priorTimestamp = 0;
        private final long maxDelay = 604800000; //1 week
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
        public void flatMap(String sentence, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] words = sentence.split("\t");
            String date = words[1];
            String eventCode = words[26];
            if((eventCode.substring(0, 2).equals("14") && eventCode.substring(2, 3).equals("5"))
                    || (eventCode.substring(0, 2).equals("17") && eventCode.substring(2, 3).equals("5"))
                    || eventCode.substring(0, 2).equals("18")
                    || eventCode.substring(0, 2).equals("19")
                    || eventCode.substring(0, 2).equals("20")) {
                out.collect(new Tuple3<>(date, eventCode, 1));
            }
        }
    }

}