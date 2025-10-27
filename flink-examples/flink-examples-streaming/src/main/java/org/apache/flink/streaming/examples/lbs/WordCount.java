package org.apache.flink.streaming.examples.lbs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        // TODO: .flatMap() 这一步和上一步`.map()`所作的事情是如出一辙的，最后得到一个新的DataStream对象
        // TODO: DataStream中的env的transformations集合现在有了两个元素了，一个是上面map得到Transformation，一个是现在flatMap得到的transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDataStream = mapDataStream.flatMap(new Splitter());
        // TODO: 还是新创建了一个类型为KeyedStream的DataStream，DataStream里面持有上一个DataStream的env、新构建的Transformation（类型为PartitionTransformation)、用户传入的分区器、key类型
        // TODO: 需要注意PartitionTransformation并没有具体的算子Operator，而是包含了用户传入的分区器、数据交互模式，它不代表具体计算任务Operator，还是两个计算任务直接连接的属性
        KeyedStream<Tuple2<String, Integer>, String> keyedDataStream = flatMapDataStream.keyBy(value -> value.f0);
        // TODO: 这一步和最开始的.socketTextStream()的内部流程步骤很类似，都是无需用户自己编写function，而是flink自己根据传入的参数构建内置function
        // TODO: 其他步骤，例如创建新的transformation并持有上一个transformation，将新的transformation加入到env中的集合中，最后使用env、新transformation创建新的DataStream都是一样的
        // TODO: 唯一有区别的地方在于，这里创建的transformation类型为ReduceTransformation，里面不持有OperatorFactory，而是直接持有function。还有就是额外要多持有分区器相关的对象
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = keyedDataStream.sum(1);
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

