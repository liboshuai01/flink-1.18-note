package org.apache.flink.streaming.examples.lbs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);
        SingleOutputStreamOperator<String> mapDataStream = dataStream.map(String::toLowerCase);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDataStream = mapDataStream.flatMap(new Splitter());
        KeyedStream<Tuple2<String, Integer>, String> keyedDataStream = flatMapDataStream.keyBy(value -> value.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowDataStream = keyedDataStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = windowDataStream.sum(1);
        DataStreamSink<Tuple2<String, Integer>> dataStreamSink = sumDataStream.print();

        env.execute("word count demo");
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(
                String sentence,
                Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

