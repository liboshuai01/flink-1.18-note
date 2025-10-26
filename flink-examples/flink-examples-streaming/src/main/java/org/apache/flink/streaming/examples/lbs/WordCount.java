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
        // TODO: Idea中启动会创建一个LocalStreamEnvironment，然后对LocalStreamEnvironment进行一些项目运行所需配置的初始化工作。
        // TODO: 此时env中只有 [并行度为cpu核心数、部署目标为local、提交作业的方式为附加模式等] 这些简单的配置信息.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO: 通过env创建了DataStream, DatStream持有env、transformation（类型为LegacySourceTransformation）, transformation持有OperatorFactory.
        // TODO: OperatorFactory持有Operator, Operator仅持有SocketTextStreamFunction.
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);
        // TODO: 通过source源的DataStream创建了一个的DataStream对象，这个全新的DataStream对象同样持有env、transformation
        // TODO: 但这次的transformation（类型为OneInputTransformation）也是新创建的，并且里面的input成员变量存放这上一个DataStream中的transformation
        // TODO: 另外还将这个新的transformation存放到了env中的transformations这个list中，以便后续生成StreamGraph图遍历使用
        // TODO: 同样这个新的transformation中也持有一个OperatorFactory, OperatorFactory又持有Operator, Operator仅持有用户传入的MapFunction
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

