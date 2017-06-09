package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * Created by Valerio Tomassi 1756285.
 */
public class trendingViolentRegions_daily {

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

        DataStream<Tuple4<String,String, String, Integer>> text = rawdata
                .flatMap(new Splitter());

        DataStream<Tuple4<String, String, String, Integer>> withTimestamp =
                text.assignTimestampsAndWatermarks(new CustomWatermark());


        DataStream<Tuple3<String, String, Integer>> grouped =
                withTimestamp
                        .map(new toDay())
                        .keyBy(1)
                        .window(TumblingEventTimeWindows.of(Time.days(1L)))
                .sum(2);

        DataStream<ArrayList<Tuple3<String,String,Integer>>> out = grouped
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1L)))
                .fold(new ArrayList<Tuple3<String,String,Integer>>(),
                        new FoldFunction<Tuple3<String,String,Integer>,
                                ArrayList<Tuple3<String,String,Integer>>>() {
                            public ArrayList<Tuple3<String,String,Integer>> fold(ArrayList<Tuple3<String,String,Integer>> current,
                                                                                 Tuple3<String,String,Integer> value) {
                                if(current.size()<10){
                                    current.add(value);
                                    return current;
                                }
                                else {

                                    Tuple3<String,String,Integer> min = current.get(9);
                                    for(Tuple3<String, String, Integer> tupla : current){
                                        if(tupla.f2 < min.f2){
                                            min = tupla;
                                        }
                                    }
                                    if(value.f2>min.f2){
                                        current.remove(min);
                                        current.add(value);
                                    }

                                    Collections.sort(current, Collections.reverseOrder(new CustomComparator()));
                                    return current;
                                }
                            }
                        });

                        out.print();


        env.execute();
    }

    private static class  CustomComparator implements Comparator<Tuple3<String,String, Integer>> {

        public int compare(Tuple3<String,String, Integer> t1, Tuple3<String, String, Integer> t2) {
            return t1.f2 - t2.f2;
        }
    }


    public static class toDay implements MapFunction<Tuple4<String,String,String, Integer>, Tuple3<String,String,Integer>> {
        public Tuple3<String,String, Integer> map(Tuple4<String,String, String, Integer> input ){
            return new Tuple3<>(input.f0,input.f2,input.f3);
        }
    }

    public static class CustomWatermark implements AssignerWithPeriodicWatermarks<Tuple4<String,String, String, Integer>>{

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
        public long extractTimestamp(Tuple4<String, String,String, Integer> tuple, long priorTimestamp) {

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
