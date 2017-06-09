package it.uniroma1.dis.bdc.ex5_HW2;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
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

public class trendingLeaders_daily {
    /**
     * Main entry point for command line execution.
     *
     * @param args the arguments as received from the command link. They are used to extract the filename of the dataset.
     * @throws Exception exceptions generated during the execution of the apache flink engine.
     */
    public static void main(final String[] args) throws Exception {


        // The ExecutionEnvironment is the context in which a program is executed.
        // A local environment will cause execution in the current JVM,
        // a remote environment will cause execution on a remote cluster installation.
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


        grouped
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1L)))
                .fold(new ArrayList<Tuple3<String,String,Integer>>(),
                        new FoldFunction<Tuple3<String,String,Integer>, ArrayList<Tuple3<String,String,Integer>>>() { public ArrayList<Tuple3<String,String,Integer>> fold(ArrayList<Tuple3<String,String,Integer>> top5,  Tuple3<String,String,Integer> value) {

                            if(top5.size()<10){
                                top5.add(value);
                                Collections.sort(top5, Collections.reverseOrder(new CustomComparator()));
                                return top5;
                            }
                            else {

                                Tuple3<String,String,Integer> min = top5.get(9);
                                if(value.f2>min.f2){
                                    top5.remove(min);
                                    top5.add(value);
                                }

                                Collections.sort(top5, Collections.reverseOrder(new CustomComparator()) );
                                return top5;
                            }
                        }
                        }).writeAsText("./output/trendingLeaders_daily", FileSystem.WriteMode.OVERWRITE);
        //.print();

        env.execute();
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

    private static class  CustomComparator implements Comparator<Tuple3<String,String, Integer>> {

        public int compare(Tuple3<String,String, Integer> t1, Tuple3<String, String, Integer> t2) {
            return t1.f2 - t2.f2;
        }
    }

    public static class Splitter implements FlatMapFunction<String, Tuple3<String, String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            String[] words = line.split("\t");
            String date = words[1];
            String leader = words[6];
            int numberOfMentions = Integer.parseInt(words[31]);

            if(!(leader.isEmpty())) {
                out.collect(new Tuple3<>(date, leader, numberOfMentions));
            }
        }
    }

}

