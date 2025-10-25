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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The LocalStreamEnvironment is a StreamExecutionEnvironment that runs the program locally,
 * multi-threaded, in the JVM where the environment is instantiated. It spawns an embedded Flink
 * cluster in the background and executes the program on that cluster.
 */
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

    /** Creates a new mini cluster stream environment that uses the default configuration. */
    public LocalStreamEnvironment() {
        this(new Configuration());
    }

    /**
     * Creates a new mini cluster stream environment that configures its local executor with the
     * given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public LocalStreamEnvironment(@Nonnull Configuration configuration) {
        super(validateAndGetConfiguration(configuration));
    }

    private static Configuration validateAndGetConfiguration(final Configuration configuration) {
        // TODO: 做了一下检查，要求如果是使用的为LocalStreamEnvironment，那么contextEnvironmentFactory和threadLocalContextEnvironmentFactory都必须为null，否则抛异常
        if (!areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, "
                            + "or running in a TestEnvironment context.");
        }
        // TODO: 配置`execution.target`为`local`（提交部署的目标为本地），`execution.attached`为true（提交方式为附加，不是分离的）
        final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        return effectiveConfiguration;
        // TODO: 到这里`StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();`这行代码就执行结束了
        // TODO: 我们可以看到本地idea启动时，就是创建了实现类LocalStreamEnvironment实例对象，然后这个实例对象中持有config和configuration两个配置对象
        // TODO: 而配置对象被初始化设置了一些诸如并行度、提交目标地、提交方式等配置参数，其他什么事情都没有做了。
    }

    @Override
    public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
        return super.execute(streamGraph);
    }
}
