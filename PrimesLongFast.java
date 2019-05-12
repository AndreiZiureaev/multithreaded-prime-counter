/**
 * Counts how many primes less than or equal to a long integer there are.
 * It uses multiple threads to count about half a billion primes per second
 * on a multicore system.
 *
 * @author Andrei Ziureaev
 */
public class PrimesLongFast {

    /**
     * The state passed to each thread.
     */
    private class Config {
        long upperLimit;
        int startingIterationIndex;
        int iterations;
        int iterationStep;
        int maxValueInIteration;
        boolean notPrime[];
        int numBasePrimes;
        int basePrimes[];
        int primeCount = 0;
    }

    private static final boolean DEBUG = false;
    private static final String USAGE =
       "\nUsage: PrimesLongFast <upper limit> [number of threads]\n" +
       "\tCounts the number of primes up to and including <upper limit>.\n" +
       "\t[number of threads] defaults to 1 and should be bigger than 0.";

    private long upperLimit;
    private int numThreads;

    private long cachedResult = -1;

    private int maxValueInIteration;
    private int iterations;
    private int numBasePrimes;
    private int[] basePrimes;
    private boolean[] notPrime;

    /**
     * @param upperLimit Count primes up to and including this number.
     * @param numThreads The number of threads to use. Should be bigger than 0.
     */
    public PrimesLongFast(long upperLimit, int numThreads) {
        this.upperLimit = upperLimit;

        if (upperLimit < 2) {
            return;
        }

        if (numThreads < 1) {
            numThreads = 1;
        }

        this.numThreads = numThreads;

        // The range of numbers is split into sections, each sqrt(upperLimit)
        // in length. A single iteration counts primes in a single section.
        maxValueInIteration = (int) Math.ceil(Math.sqrt(upperLimit));

        // How many iterations of size maxValueInIteration can fit into the
        // range? Each iteration is independent from the others (except the
        // first one) and primes in each iteration can be counted in their
        // own thread.
        iterations = (int) (upperLimit / maxValueInIteration);

        notPrime = new boolean[maxValueInIteration + 1];
        notPrime[0] = true; // All elements are initially false.
        notPrime[1] = true;

        // All the primes of the first iteration will be stored here.
        basePrimes = new int[findSize(maxValueInIteration)];
        numBasePrimes = 0;
    }

    /**
     * Initiates the counting.
     *
     * @return The number of primes.
     */
    public long countPrimes() {
        if (cachedResult >= 0) {
            return cachedResult;
        }

        if (upperLimit < 2) {
            return 0;
        }

        calculateBasePrimes();

        if (DEBUG) System.out.println(
            "Found the base primes. Now counting the rest...\n\n" +
            "maxValueInIteration = " + maxValueInIteration +
            "; iterations = " + iterations +
            "; numBasePrimes = " + numBasePrimes + ";\n"
        );

        cachedResult = countOtherPrimes();

        return cachedResult;
    }

    /**
     * Calculates the primes up to the square root of upperLimit and stores them
     * in basePrimes.
     */
    private void calculateBasePrimes() {
        int prime = 2; // 2 is the first prime.
        int multiple;

        // Find primes up to the square root of
        // maxValueInIteration, marking their multiples as non-prime. These
        // multiples cover the non-primes of the whole iteration up to
        // maxValueInIteration.
        int upper = (int) Math.sqrt(maxValueInIteration);

        while (prime <= upper) {

            // All the multiples of prime are not prime.
            // Mark them in notPrime[].
            // Starting from prime^2 because all the non-primes less than that
            // would have been marked already.
            multiple = prime * prime;

            while (multiple <= maxValueInIteration) {
                notPrime[multiple] = true;
                multiple += prime;
            }

            // prime becomes the next prime.
            do {
                prime++;
            } while (notPrime[prime]); // All the non-primes known so far are
                                       // marked true in notPrime[].
        }

        // Fill in basePrimes[].
        for (int i = 2; i <= maxValueInIteration; i++) {
            if (notPrime[i] == false) {
                basePrimes[numBasePrimes++] = i;
            }
        }
    }

    /**
     * Run multiple threads that use the base primes to count all the other
     * primes.
     *
     * @return The total number of primes.
     */
    private long countOtherPrimes() {
        long primeCount = numBasePrimes;
        Thread[] threads = new Thread[numThreads];
        Config[] configs = new Config[numThreads];

        for (int i = 0; i < numThreads; i++) {
            configs[i] = new Config();
            configs[i].upperLimit = upperLimit;
            configs[i].startingIterationIndex = i + 1;
            configs[i].iterations = iterations;
            configs[i].iterationStep = numThreads;
            configs[i].maxValueInIteration = maxValueInIteration;
            configs[i].notPrime = new boolean[maxValueInIteration + 1];
            configs[i].numBasePrimes = numBasePrimes;
            configs[i].basePrimes = basePrimes;

            // Arrow functions can only access final, or effectively final
            // variables.
            Config c = configs[i];

            threads[i] = new Thread(() -> {
                iterate(c);
            });
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                System.err.println("Interrupted");
            }

            // Extract the finished thread's primeCount and add it to the total.
            primeCount += configs[i].primeCount;
        }

        return primeCount;
    }

    /**
     * Counts the primes according to the parameters in c and stores the count
     * in c.primeCount.
     *
     * @param c The state required to count the primes.
     */
    private static void iterate(Config c) {
        int i;

        for (i = c.startingIterationIndex; i < c.iterations; i += c.iterationStep) {
            performIteration(c, i * (long) c.maxValueInIteration);
        }

        // Final iteration (if it's needed) is shorter than the rest.
        long start = i * (long) c.maxValueInIteration;

        if (start < c.upperLimit) {
            int newMaxValueInIteration = (int) (c.upperLimit - start);
            c.maxValueInIteration = newMaxValueInIteration;

            if (DEBUG) System.out.println(
                "Final iteration...\n\n" +
                "newMaxValueInIteration = " + newMaxValueInIteration +
                "; i = " + i + "; start = " + start + ";\n"
            );

            performIteration(c, start);
        }
    }

    /**
     * Counts the primes in this iteration and stores the count in c.primeCount.
     *
     * @param c     The state required to count the primes in this iteration.
     * @param start The lowest possible prime in this iteration is start + 1.
     */
    private static void performIteration(Config c, long start) {

        // start + startMod2 = an even number
        int startMod2 = (int) (start % 2);

        // Ignore half the values because they are even.
        // Set the rest to false because they could be primes.
        // If start is even, the first possible prime is start + 1.
        // If start is odd, the first possible prime after start is
        // start + 2.
        for (int k = startMod2 + 1; k <= c.maxValueInIteration; k += 2) {
            c.notPrime[k] = false;
        }

        // Iterate over the primes found beforehand. Start at 1 because
        // the first prime is 2, which is already dealt with.
        for (int j = 1; j < c.numBasePrimes; j++) {

            int primeX2 = c.basePrimes[j] * 2;

            // start + offset = m
            // where m is the first multiple of basePrimes[j]
            // bigger than start.
            // 1 <= offset <= basePrimes[j]
            int offset = c.basePrimes[j] - (int) (start % c.basePrimes[j]);

            // No need to consider even multiples. Set up the offset
            // so that it falls on an odd multiple.
            // If start is even, then offset has to be odd and vice versa.
            if (offset % 2 == startMod2) {
                offset += c.basePrimes[j];
            }

            // offset goes through all the odd multiples of the prime
            while (offset <= c.maxValueInIteration) {
                c.notPrime[offset] = true;
                offset += primeX2;
            }
        }

        // Add the primes in this iteration to the count
        for (int k = startMod2 + 1; k <= c.maxValueInIteration; k += 2) {
            if (c.notPrime[k] == false) {
                c.primeCount++;
            }
        }
    }

    /**
     * See https://primes.utm.edu/howmany.html
     *
     * @param maxValueInIteration The biggest value in the first iteration.
     * @return The upper limit to the number of primes in the first iteration.
     */
    private static int findSize(int maxValueInIteration) {
        return (int) (
            (maxValueInIteration / Math.log(maxValueInIteration)) *
            (1 + 1.2762 / Math.log(maxValueInIteration))
        ) + 1;
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        if (args.length < 1 || args.length > 2) {
            System.out.println(USAGE);
            return;
        }

        long upperLimit;
        try {
            upperLimit = Long.parseLong(args[0]);
        } catch (NumberFormatException e) {
            System.out.println(USAGE);
            return;
        }

        int numThreads = 1;
        if (args.length > 1) {
            try {
                numThreads = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println(USAGE);
                return;
            }

            if (numThreads < 1) {
                System.out.println(USAGE);
                return;
            }
        }

        PrimesLongFast p = new PrimesLongFast(upperLimit, numThreads);
        long count = p.countPrimes();

        System.out.println(
            (
                count == 1 ?
                "There is 1 prime less than or equal to " :
                "There are " + count + " primes less than or equal to "
            ) + upperLimit
        );

        System.out.println(
            "\nTime: " + ((float) (System.currentTimeMillis() - time) / 1000) +
            " s"
        );
    }
}
