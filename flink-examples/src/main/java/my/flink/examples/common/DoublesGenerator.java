package my.flink.examples.common;

/**
 * Created by affo on 24/11/17.
 */
public class DoublesGenerator extends AbstractGenerator<Double> {
    public DoublesGenerator(int range) {
        super(range);
    }

    @Override
    public Double fromIndexInRange(int index) {
        double next = index - random.nextDouble();
        return Math.abs(next);
    }
}
