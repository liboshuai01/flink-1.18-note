/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

    private final boolean isParallel;

    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName) {
        this(
                environment,
                outTypeInfo,
                operator,
                isParallel,
                sourceName,
                Boundedness.CONTINUOUS_UNBOUNDED);
    }

    /** The constructor used to create legacy sources. */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            TypeInformation<T> outTypeInfo,
            StreamSource<T, ?> operator,
            boolean isParallel,
            String sourceName,
            Boundedness boundedness) {
        // TODO: 将environment、transformation都存放DataStreamSource自己的成员变量中（最终是通过父类DataStream的构造方法来完成的）
        super(
                environment,
                // TODO: 将operator传入来创建一个LegacySourceTransformation
                // TODO: 这样就形成了DataStream（对应DataStreamSource）持有Transformation（对应LegacySourceTransformation），
                // TODO: 而Transformation又持有OperatorFactory (中的Operator对应StreamSource）的链条
                new LegacySourceTransformation<>(
                        sourceName,
                        operator,
                        outTypeInfo,
                        environment.getParallelism(),
                        boundedness,
                        false));
        // TODO: 将是否为并行source存入自己的成员变量中，isParallel是DataStreamSource与父类SingleOutputStreamOperator的唯一区别（多出的）
        this.isParallel = isParallel;
        // TODO: 如果不为并行source，则直接设置并行度为1
        if (!isParallel) {
            // TODO: 调用父类SingleOutputStreamOperator的方法，将并行度设置到自己持有的transformation中
            setParallelism(1);
        }
        // TODO: 到这里WordCount中的`DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);`这行代码就已经执行完毕了
        // TODO: 最后我们来总结一下`DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777);`这行代码都做了哪些事情：
        // TODO: 1. 创建了一个DataStream，并往DataStream中设置了environment和transformation
        // TODO: 2. transformation中又持有一个OperatorFactory（内部的Operator为StreamSource）
        // TODO: 3. OperatorFactory中又持有Operator（名为StreamSource）是通过flink自己最开始自带的一个SocketTextStreamFunction构建出来的

        // TODO: DataStream就是一个面向用户的api，方便用户进行链式编程。用户.map().filter()最后都会形成一个个transformation存放在DataStream的集合中。
        // TODO: transformation是最后用于执行作业的详细施工图，它里面有并行度、结果输出类型、是否为无界流和最重要的Operator，后面会根据transforation合集创建StreamGraph。
        // TODO: Operator是具体的施工队，干活的工人。它最后会被StreamTask调用来其中用户传入的function来进行数据处理。
    }

    /**
     * Constructor for "deep" sources that manually set up (one or more) custom configured complex
     * operators.
     */
    public DataStreamSource(SingleOutputStreamOperator<T> operator) {
        super(operator.environment, operator.getTransformation());
        this.isParallel = true;
    }

    /** Constructor for new Sources (FLIP-27). */
    public DataStreamSource(
            StreamExecutionEnvironment environment,
            Source<T, ?, ?> source,
            WatermarkStrategy<T> watermarkStrategy,
            TypeInformation<T> outTypeInfo,
            String sourceName) {
        super(
                environment,
                new SourceTransformation<>(
                        sourceName,
                        source,
                        watermarkStrategy,
                        outTypeInfo,
                        environment.getParallelism(),
                        false));
        this.isParallel = true;
    }

    @VisibleForTesting
    boolean isParallel() {
        return isParallel;
    }

    @Override
    public DataStreamSource<T> setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, isParallel);
        super.setParallelism(parallelism);
        return this;
    }
}
