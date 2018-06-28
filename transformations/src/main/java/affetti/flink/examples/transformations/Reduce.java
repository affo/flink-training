package affetti.flink.examples.transformations;

import affetti.flink.examples.common.RandomIntegersSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by affo on 22/11/17.
 */
public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integers = env.addSource(new RandomIntegersSource(10));
        integers
                .map((i) -> i * i)
                .keyBy((i) -> 0)
                .reduce((accumulator, i) -> accumulator + i)
                .print();

        env.execute();

        System.out.println("\n>>> Alternatively:\n");

        integers
                .map((i) -> Tuple2.of(0, i * i))
                .returns(new TypeHint<Tuple2<Integer, Integer>>() {
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
