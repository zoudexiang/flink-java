package com.common.flink.table.join;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2024/9/28 13:47
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class FlinkSQLJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 默认值 10, 表示 FlinkSQL 中的状态永久保存
        // 每条数据来了之后 10s 后失效
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<TableA> aDS = env.socketTextStream("bigdata201", 8888).map(
                line -> {
                    String[] split = line.split(",");
                    return new TableA(split[0], split[1]);
                }
        );

        SingleOutputStreamOperator<TableB> bDS = env.socketTextStream("bigdata201", 9999).map(
                line -> {
                    String[] split = line.split(",");
                    return new TableB(split[0], Integer.parseInt(split[1]));
                }
        );

        tableEnv.createTemporaryView("tableA", aDS);
        tableEnv.createTemporaryView("tableB", bDS);

        // 双流 join
        tableEnv.sqlQuery("select * from tableA a join tableB b on a.id=b.id").execute().print();

        env.execute();
    }
}
