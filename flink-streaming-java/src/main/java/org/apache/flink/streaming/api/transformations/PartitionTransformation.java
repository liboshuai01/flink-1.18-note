/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

// TODO: PartitionTransformation 本身不代表一个计算任务（即一个算子），而是定义了两个计算任务（Transformation 节点）之间数据连接（边，Edge）的属性
// TODO: PartitionTransformation中并没有包含实际的OperatorFactory，而OneInputTransformation、TwoInputTransformatio都有
// TODO: 但是它多出了两个成员变量partitioner（分区器）、exchangeMode（数据交换模式）
/**
 * This transformation represents a change of partitioning of the input elements.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@link PartitionTransformation}
 */
@Internal
public class PartitionTransformation<T> extends Transformation<T> {

    // TODO: 前一个DataStream中的transformation
    private final Transformation<T> input;

    // TODO: 数据分发的具体逻辑，上游算子产生的每一条数据记录，应该被发送到下游算子的哪一个并行实例
    private final StreamPartitioner<T> partitioner;

    // TODO: 数据在物理上如何从上游 Task 传输到下游 Task，数据应该立即发送，还是等上游任务处理完一批（或全部）数据后再发送
    private final StreamExchangeMode exchangeMode;

    /**
     * Creates a new {@link PartitionTransformation} from the given input and {@link
     * StreamPartitioner}.
     *
     * @param input The input {@link Transformation}
     * @param partitioner The {@link StreamPartitioner}
     */
    public PartitionTransformation(Transformation<T> input, StreamPartitioner<T> partitioner) {
        this(input, partitioner, StreamExchangeMode.UNDEFINED);
    }

    /**
     * Creates a new {@link PartitionTransformation} from the given input and {@link
     * StreamPartitioner}.
     *
     * @param input The input {@link Transformation}
     * @param partitioner The {@link StreamPartitioner}
     * @param exchangeMode The {@link StreamExchangeMode}
     */
    public PartitionTransformation(
            Transformation<T> input,
            StreamPartitioner<T> partitioner,
            StreamExchangeMode exchangeMode) {
        super("Partition", input.getOutputType(), input.getParallelism());
        this.input = input;
        this.partitioner = partitioner;
        this.exchangeMode = checkNotNull(exchangeMode);
    }

    /**
     * Returns the {@link StreamPartitioner} that must be used for partitioning the elements of the
     * input {@link Transformation}.
     */
    public StreamPartitioner<T> getPartitioner() {
        return partitioner;
    }

    /** Returns the {@link StreamExchangeMode} of this {@link PartitionTransformation}. */
    public StreamExchangeMode getExchangeMode() {
        return exchangeMode;
    }

    @Override
    protected List<Transformation<?>> getTransitivePredecessorsInternal() {
        List<Transformation<?>> result = Lists.newArrayList();
        result.add(this);
        result.addAll(input.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }
}
