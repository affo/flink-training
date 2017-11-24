package my.flink.examples.common;

/**
 * Created by affo on 22/11/17.
 */
public class IntegersGenerator extends AbstractGenerator<Integer> {

    public IntegersGenerator(int range) {
        super(range);
    }

    @Override
    public Integer fromIndexInRange(int index) {
        return index;
    }
}
