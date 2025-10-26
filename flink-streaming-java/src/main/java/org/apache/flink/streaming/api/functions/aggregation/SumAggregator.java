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

package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

/** An {@link AggregationFunction} that sums up fields. */
@Internal
public class SumAggregator<T> extends AggregationFunction<T> {

    private static final long serialVersionUID = 1L;

    /**
     * 字段访问器，用于统一地从数据元素 T 中读取和写入目标字段的值。
     * 它可以屏蔽底层数据结构（如 POJO、Tuple）的差异。
     */
    private final FieldAccessor<T, Object> fieldAccessor;
    /**
     * 实际执行求和操作的函数。
     * Flink 会根据目标字段的具体数值类型（如 Integer, Double, Long 等）选择对应的 SumFunction 实现。
     */
    private final SumFunction adder;
    /**
     * 用于创建元素 T 的深拷贝（deep copy）。
     * 这对于 POJO 等非元组类型至关重要，以确保在 reduce 操作中不会修改原始对象。
     * 对于 Tuple 类型，我们会使用其自带的 copy() 方法，因此该字段可能为 null。
     */
    private final TypeSerializer<T> serializer;
    /**
     * 一个优化标志，用于标识输入类型 T 是否为 Tuple。
     * 如果是，我们可以使用更高效的 `tuple.copy()` 方法；否则，必须使用通用的 `serializer.copy()`。
     */
    private final boolean isTuple;

    /**
     * 构造函数，用于通过字段位置索引来创建 SumAggregator。
     *
     * @param pos      要求和的字段的位置索引（从 0 开始）。
     * @param typeInfo 输入元素的类型信息。
     * @param config   Flink 作业的执行配置。
     */
    public SumAggregator(int pos, TypeInformation<T> typeInfo, ExecutionConfig config) {
        // TODO: 1. 使用工厂类创建与该位置索引相匹配的字段访问器
        // TODO: typeInfo = Java Tuple2<String, Integer>, pos = 1
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        // TODO: 2. 根据字段访问器推断出的字段类型（如 Integer.class），获取对应的求和函数
        // TODO: fieldAccessor.getFieldType().getTypeClass() = Integer.class
        adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        // TODO: 3. 判断输入类型是否为 Tuple，以决定后续使用哪种拷贝方式
        if (typeInfo instanceof TupleTypeInfo) {
            isTuple = true;
            serializer = null; // Tuple 类型不需要通用序列化器进行拷贝
        } else {
            isTuple = false;
            this.serializer = typeInfo.createSerializer(config); // 为 POJO 等类型创建序列化器
        }
    }

    /**
     * 构造函数，用于通过字段名称来创建 SumAggregator。
     *
     * @param field    要求和的字段的名称（支持嵌套，如 "user.score"）。
     * @param typeInfo 输入元素的类型信息。
     * @param config   Flink 作业的执行配置。
     */
    public SumAggregator(String field, TypeInformation<T> typeInfo, ExecutionConfig config) {
        // 逻辑与按位置索引的构造函数基本一致
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, field, config);
        adder = SumFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        if (typeInfo instanceof TupleTypeInfo) {
            isTuple = true;
            serializer = null;
        } else {
            isTuple = false;
            this.serializer = typeInfo.createSerializer(config);
        }
    }

    /**
     * reduce 操作的核心逻辑。它接收两个元素，将它们的指定字段相加，
     * 然后将结果设置到一个新的元素副本中并返回。
     *
     * @param value1 第一个值（通常是当前的聚合结果）
     * @param value2 第二个值（新流入的元素）
     * @return 聚合后的新值
     */
    @Override
    @SuppressWarnings("unchecked")
    public T reduce(T value1, T value2) throws Exception {
        if (isTuple) {
            Tuple result = ((Tuple) value1).copy();
            return fieldAccessor.set(
                    (T) result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
        } else {
            T result = serializer.copy(value1);
            return fieldAccessor.set(
                    result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
        }
    }
}
