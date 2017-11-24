package my.flink.examples.transformations;

import my.flink.examples.common.RandomIntegersSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Created by affo on 22/11/17.
 */
public class UnionConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.addSource(new RandomIntegersSource(10));
        DataStreamSource<Integer> s2 = env.addSource(new RandomIntegersSource(10));

        DataStream<Tuple2<Integer, Integer>> withOnes = s1
                .union(s2)
                .map(i -> Tuple2.of(i, 1))
                .returns(new TypeHint<Tuple2<Integer, Integer>>() {
                });

        withOnes
                .keyBy(1)
                .sum(1)
                .project(1)
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively:\n");

        s1.connect(s2)
                .map(
                        new CoMapFunction<Integer, Integer, Integer>() {
                            private int count = 0;

                            @Override
                            public Integer map1(Integer integer) throws Exception {
                                return ++count;
                            }

                            @Override
                            public Integer map2(Integer integer) throws Exception {
                                return ++count;
                            }
                        }
                )
                .print();

        env.execute();
    }
}
