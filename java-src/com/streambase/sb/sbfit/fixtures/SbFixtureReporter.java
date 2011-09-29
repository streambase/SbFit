package com.streambase.sb.sbfit.fixtures;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.TreeMap;

public class SbFixtureReporter {
    public static final SbFixtureReporter reporter = new SbFixtureReporter();
    private static final String REPORT_FILE = System.getProperty(
            "sbfit.stat-file", "sbfit-timing-report.log");

    static {
        // always add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    reporter.report();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static double seconds(long nanos) {
        return nanos / 1.0e9;
    }

    private static class Timer {
        private long totalTime = 0;
        private long curTime = 0;

        public void start() {
            if (curTime == 0) {
                curTime = System.nanoTime();
            }
        }

        public void stop() {
            assert curTime != 0;
            totalTime += System.nanoTime() - curTime;
            curTime = 0;
        }
    }

    private final Map<String, Timer> fixtureTimes = new TreeMap<String, Timer>();

    public void start(String type) {
        Timer t = fixtureTimes.get(type);
        if (t == null) {
            t = new Timer();
            fixtureTimes.put(type, t);
        }
        t.start();
    }

    public void stop(String type) {
        Timer t = fixtureTimes.get(type);
        assert t != null;
        t.stop();
    }

    public void report() throws FileNotFoundException {
        double totalTime = ManagementFactory.getRuntimeMXBean().getUptime() / 1.0e3;

        PrintStream p = new PrintStream(REPORT_FILE);
        p.printf("Total Time: %01.3f seconds\n", totalTime);

        for (Map.Entry<String, Timer> e : fixtureTimes.entrySet()) {
            double seconds = seconds(e.getValue().totalTime);
            double percent = seconds / totalTime;
            p.printf("%s: %01.3f seconds (%02.1f%%)\n", e.getKey(), seconds,
                    percent * 100);
        }
        p.close();
    }

}
