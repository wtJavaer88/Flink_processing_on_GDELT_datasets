package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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

/**
 * Created by Valerio Tomassi 1756285.
 */
public class trendingViolentRegions_weekly {

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

        DataStream<Tuple4<String, String, String, Integer>> text = rawdata
                .flatMap(new Splitter());

        DataStream<Tuple4<String, String, String, Integer>> marked =
                text.assignTimestampsAndWatermarks(new myWatermark());

        DataStream<Tuple3<String, String, ArrayList<Tuple2<Integer,Integer>>>> grouped =
                marked
                        .map(new removeField())
                        .keyBy(1)
                        .window(TumblingEventTimeWindows.of(Time.days(7L)))
                        .apply(new WindowFunction<Tuple3<String, String, Integer>, Tuple3<String, String, ArrayList<Tuple2<Integer,Integer>>>, Tuple, TimeWindow>() {
                            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple3<String, String, Integer>> iterable,
                                              Collector<Tuple3<String, String, ArrayList<Tuple2<Integer,Integer>>>> collector) throws Exception {

                                String region = "";
                                String date = "";

                                ArrayList<Tuple2<Integer,Integer>> mentions = new ArrayList<>();

                                for(Tuple3<String, String, Integer> t  : iterable) {
                                    Tuple2<Integer, Integer> temp = new Tuple2<>(Integer.parseInt(t.f0), t.f2);
                                    mentions.add(temp);
                                    region = t.f1;
                                    date = t.f0;
                                }

                                Calendar calendar = Calendar.getInstance();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

                                Date d = sdf.parse(date);
                                calendar.setTime(d);
                                int week = calendar.get(Calendar.WEEK_OF_YEAR);

                                Collections.sort(mentions, new CustomComparatorByMentions());

                                collector.collect(new Tuple3<>(String.valueOf(week), region, mentions));
                            }
                        });

        DataStream<Tuple3<String, String, Integer>> trending =
                grouped.map(new MapFunction<Tuple3<String, String, ArrayList<Tuple2<Integer,Integer>>>, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> map(Tuple3<String, String, ArrayList<Tuple2<Integer,Integer>>> tuple) throws Exception {

                        int trend = 0;
                        int prev;
                        int cur = 0;

                        for(Tuple2<Integer,Integer> t : tuple.f2){
                            prev = cur;
                            cur = t.f1;
                            trend += cur - prev;
                        }
                        return new Tuple3<>(tuple.f0, tuple.f1, trend);
                    }
                });

        trending
                .keyBy(0)
                .windowAll(TumblingEventTimeWindows.of(Time.days(7L)))
                .fold(new ArrayList<Tuple3<String,String,Integer>>(),
                        new FoldFunction<Tuple3<String,String,Integer>,
                                ArrayList<Tuple3<String,String,Integer>>>() {
                            public ArrayList<Tuple3<String,String,Integer>> fold(ArrayList<Tuple3<String,String,Integer>> tuples,
                                                                                 Tuple3<String,String,Integer> tuple) {
                                if(tuples.size()<10){
                                    tuples.add(tuple);
                                    return tuples;
                                }
                                else {

                                    Tuple3<String,String,Integer> min = tuples.get(9);

                                    for(Tuple3<String, String, Integer> t : tuples){
                                        if(t.f2 < min.f2){
                                            min = t;
                                        }
                                    }
                                    if(tuple.f2>min.f2){
                                        tuples.remove(min);
                                        tuples.add(tuple);
                                    }

                                    Collections.sort(tuples, Collections.reverseOrder(new CustomComparator()) );
                                    return tuples;
                                }
                            }
                        }).print();


        env.execute();
    }

    private static class  CustomComparator implements Comparator<Tuple3<String,String, Integer>> {

        public int compare(Tuple3<String,String, Integer> t1, Tuple3<String, String, Integer> t2) {
            return t1.f2 - t2.f2;
        }
    }

    public static class removeField implements MapFunction<Tuple4<String,String,String, Integer>, Tuple3<String,String,Integer>> {
        public Tuple3<String,String, Integer> map(Tuple4<String,String, String, Integer> input ){
            return new Tuple3<>(input.f0,input.f2,input.f3);
        }
    }

    private static class CustomComparatorByMentions implements Comparator<Tuple2<Integer, Integer>> {

        public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
            return t1.f0 - t2.f0;
        }
    }

    public static class myWatermark implements AssignerWithPeriodicWatermarks<Tuple4<String, String, String, Integer>> {

        private static final long serialVersionUID = 1L;
        private long maxTimestamp = 0;
        private long priorTimestamp = 0;
        private final long maxDelay = 604800000; //1 week



        public Watermark getCurrentWatermark() {
            return new Watermark(this.maxTimestamp);
        }


        public long extractTimestamp(Tuple4<String, String, String, Integer> mytuple, long priorTimestamp) {
            Calendar calendar = GregorianCalendar.getInstance();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            Date date = null;
            try {
                date = format.parse(mytuple.f0);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            calendar.setTime(date);
            maxTimestamp = Math.max(calendar.getTimeInMillis(), priorTimestamp);
            return calendar.getTimeInMillis();
        }
    }


    public static class Splitter implements FlatMapFunction<String, Tuple4<String, String, String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple4<String, String, String,  Integer>> out) throws Exception {
            String[] words = sentence.split("\t");
            String date = words[1];
            String eventCode = words[26];
            String region = words[44];
            int mentions = Integer.parseInt(words[31]);
            String type = "";
            if((eventCode.substring(0, 2).equals("14") && eventCode.substring(2, 3).equals("5"))
                    || (eventCode.substring(0, 2).equals("17") && eventCode.substring(2, 3).equals("5"))
                    || eventCode.substring(0, 2).equals("18")
                    || eventCode.substring(0, 2).equals("19")
                    || eventCode.substring(0, 2).equals("20")) {
                type=  "VIOLENT";
            }
            else
                type = "NON_VIOLENT";

            out.collect(new Tuple4<>(date, type, region, mentions));
        }
    }
}
