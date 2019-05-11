import java.lang.Math;

public class PrimesLongFast {

	// The state passed to each thread
	private class Config {
        int iStart;
        int iterations;
        int iStep;
        int maxNum;
        int kStart;
        boolean notPrime[];
        int position;
        int primes[];
        int evenness;
        int primeCount = 0;
    }

	private static final String ERROR_WRONG_FORMAT =
	   "\nUsage: PrimesLongFast <integer>\nFinds primes up to <integer>.\n";

	private long initial;
    private long primeCount;
    private long primeCount2;

	private int maxNum;
    private int maxNum2;
	private int position;
    private int position2;
	private int iterations;
    private int iterations2;

	private int[] primes;
	private boolean[] notPrime, notPrime2;

	public static void main(String[] str) {

		long time = System.currentTimeMillis();

		if (str.length > 0) {

			long num = 0;

			try {
				num = Long.parseLong(str[0]);
			} catch (NumberFormatException e) {
				System.err.println(ERROR_WRONG_FORMAT);
				System.exit(1);
			}

			PrimesLongFast primesLong = new PrimesLongFast(num);
			System.out.println(
				"There are " +
                primesLong.findPrimes() +
                " primes less than or equal to " +
                num
			);

			System.out.println(
				"\nTime: " +
                ((float)(System.currentTimeMillis() - time) / 1000) +
                " s\n\n"
			);
		} else {
			System.out.println(ERROR_WRONG_FORMAT);
		}
    }

	PrimesLongFast(long num) {
		initial = num;
	}

	private long findPrimes() {

		if (!setup()) {
			return 0;
		}

		int index = 2; // 2 is the first prime.
		int index2;

        // The range of numbers is split into sections, or windows. Each
		// window is maxNum numbers in size. The first iteration finds primes
		// up to the square root of maxNum, marking their multiples as
		// non-prime. The multiples then cover the non-primes of the whole
		// window up to maxNum.
		int sqrtL = (int)Math.sqrt(maxNum);

		while (index <= sqrtL) {

            // index is prime. All its multiples are not prime.
            // Mark them in notPrime[].
			index2 = index * index;
			while (index2 <= maxNum) {
				notPrime[index2] = true;
				index2 += index;
			}

            // index becomes the next prime.
			do {
                index++;
			} while (notPrime[index]); // All the non-primes known so far are
                                       // marked true in notPrime[].
		}

        // Increase primeCount by the number of new primes found.
		count();

		System.out.println(
			"Counting...\n\nmaxNum = " +
            maxNum +
            "; iterations = " +
            iterations +
            ";\n\n"
		);

        // Run multiple threads that use the primes found in the first iteration
        // to find all the other primes.
		extend();

		return primeCount;
	}

	private boolean setup() {

		if (initial < 2) {
			return false;
		}

        // The maximum for the first iteration.
		maxNum = (int)Math.ceil(Math.sqrt(initial));
		maxNum2 = maxNum;

        // How many windows of size maxNum can fit into the range?
        // Each window is independent from the others and primes in each window
        // can be found in their own thread.
		iterations = (int)(initial / maxNum);
		iterations2 = iterations;

		primeCount = 0;
		primeCount2 = 0;

		notPrime = new boolean[maxNum + 1];
		notPrime[0] = true; // All elements are initially false. This makes 2
		notPrime[1] = true; // the first prime.
		notPrime2 = new boolean[maxNum + 1];

        // All the primes of an iteration will be stored here.
		primes = new int[findSize()];
		position = 0;

		return true;
	}

    // Returns the upper limit to the number of primes in each iteration.
	private int findSize() {
		return (int)((maxNum / Math.log(maxNum)) *
            (1 + 1.2762 / Math.log(maxNum))) + 1;
	}

	private void count() {
		for (int i = 2; i <= maxNum; i++) {
			if (notPrime[i] == false) {
				primeCount++;
				primes[position] = i;
				position++;
			}
		}
		position2 = position;
	}

	private void extend() {
		Config cfg = new Config();
		cfg.iStart = 2;
		cfg.iterations = iterations2;
		cfg.iStep = 2;
		cfg.maxNum = maxNum2;
		cfg.kStart = 1;
		cfg.notPrime = notPrime2;
		cfg.position = position2;
		cfg.primes = primes;
		cfg.evenness = 0;

		Thread t = new Thread(() -> {
			iterate(cfg);
			primeCount2 += cfg.primeCount;
		});
		t.start();

        Config[] cfgs = new Config[2];

        for (int i = 0; i < 2; i++) {
            cfgs[i] = new Config();
            cfgs[i].iStart = 1;
            cfgs[i].iterations = iterations;
            cfgs[i].iStep = 2;
            cfgs[i].maxNum = maxNum;
            cfgs[i].kStart = i + 1;
            cfgs[i].notPrime = notPrime;
            cfgs[i].position = position;
            cfgs[i].primes = primes;
            cfgs[i].evenness = i;
        }

		if (maxNum % 2 == 0) {
            iterate(cfgs[0]);
		} else {
            iterate(cfgs[1]);
		}

        primeCount += cfgs[0].primeCount + cfgs[1].primeCount;

        long start;
        int offset;

		// final iteration
		start = (long)iterations * maxNum;

		if (start < initial) {

			maxNum = (int)(initial - start);

			System.out.println(
				"Performing final iteration...\n\n" +
                "start = " +
                start +
                "; maxNum = " +
                maxNum +
                ";\n\n"
			);

			for (int k = 1; k <= maxNum; k++) {
				notPrime[k] = false;
			}

			for (int j = 0; j < position; j++) { // primes

				offset = primes[j] - (int)(start % primes[j]);

				while (offset <= maxNum) {
					notPrime[offset] = true;
					offset += primes[j];
				}
			}

			for (int k = 1; k <= maxNum; k++) { // count
				if (notPrime[k] == false) {
					primeCount++;
				}
			}
		}

		try {
			t.join();
		} catch (InterruptedException e) {
			System.out.println("Interrupted");
		}

		primeCount += primeCount2;
	}

    private static void iterate(Config cfg) {
        long start;
        int offset;
        int primesX;

        for (int i = cfg.iStart; i < cfg.iterations; i += cfg.iStep) {

            start = i * (long)cfg.maxNum;

            for (int k = cfg.kStart; k <= cfg.maxNum; k += 2) {
                cfg.notPrime[k] = false;
            }

            for (int j = 1; j < cfg.position; j++) { // primes

                primesX = cfg.primes[j] * 2;
                offset = cfg.primes[j] - (int)(start % cfg.primes[j]);

                if (offset % 2 == cfg.evenness) {
                    offset += cfg.primes[j];
                }

                while (offset <= cfg.maxNum) {
                    cfg.notPrime[offset] = true;
                    offset += primesX;
                }
            }

            for (int k = cfg.kStart; k <= cfg.maxNum; k += 2) { // count
                if (cfg.notPrime[k] == false) {
                    cfg.primeCount++;
                }
            }
        }
    }
}
