package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class Protests_daily {
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



            DataStream<Tuple2<String, Integer>> grouped =
                    withTimestamp
                    .keyBy(1)
                    .window(TumblingEventTimeWindows.of(Time.days(1L)))
                    .sum(2)
                    .map(new toDayANDCount());

            grouped.writeAsText("./output/Protests_daily");
                    //.print();

            env.execute();
         }


    public static class toDayANDCount implements MapFunction<Tuple3<String,String, Integer>, Tuple2<String,Integer>> {
        public Tuple2<String, Integer> map(Tuple3<String, String, Integer> input ){
            return new Tuple2<>(input.f0,input.f2);
        }
    }


    public static class CustomWatermark implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Integer>>{

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
        public void flatMap(String sentence, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] words = sentence.split("\t");
            String date = words[1];
            String eventCode = words[28];
            if(eventCode.equals("14")){
                out.collect(new Tuple3<>(date, eventCode, 1));
            }
        }
    }

}

