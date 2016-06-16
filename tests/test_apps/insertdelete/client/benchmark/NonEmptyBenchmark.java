package benchmark;

/**
 * Exactly like Benchmark, except that the default value for <i>empty</i> is false.
 */
public class NonEmptyBenchmark extends Benchmark {

    public NonEmptyBenchmark(String[] args, boolean emptyDefault) throws Exception {
        super(args, emptyDefault);
    }

    public NonEmptyBenchmark(String[] args) throws Exception {
        this(args, false);
    }

    public static void main(String[] args) throws Exception {

        Benchmark benchmark = new NonEmptyBenchmark(args);
        benchmark.init();
        benchmark.runBenchmark();

    }
}
