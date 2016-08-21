package com.calc;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculator that keeps state and executes calculation for specific key.
 */
class CalculatorForKey {

    private static final long CALCULATION_STEP = 100;
    private final Short key;
    private final State stateCache = new State();
    private final static Random rand = new Random();
    final List<OperationQueue> opQueues = new ArrayList<>();

    CalculatorForKey(Short key) {
        this.key = key;
    }

    /**
     * Run calculation.
     * Each operation is put in queue, that is formed for a time period of 200ms. After that, calculations are run
     * and result is returned for every thread that is in this queue.
     * In queue, all operation are ordered depending on their timestamp.
     * Since it's allowable for thread to enter calculation method 100ms later than it's ts, we have to gather queue
     * for twice that time.
     * <p>
     * In order to not waste resources, calculations itself run in the thread that was first in queue.
     */
    Double calc(Long ts, Map<String, Object> data) throws Exception {
        if (System.currentTimeMillis() - ts > CALCULATION_STEP) {
            System.out.println("Skipped calculation due to expiration. Key = " + key);
            return 0d;
        }
        Operation op = new Operation(key, ts, data);
        OperationQueue opQueue = null;
        synchronized (opQueues) {
            for (OperationQueue q : opQueues) {
                if (q.isFitting(op)) {//find a fitting queue, if there is one
                    opQueue = q;
                    break;
                }
            }
            if (opQueues.size() > 30) {//cleanup closed queues
                for (int i = 0; i < opQueues.size(); i++)
                    if (!opQueues.get(i).isOpen()) {
                        opQueues.remove(i);
                        i--;
                    }
            }
            if (opQueue == null) {//if queue wasn't found, create new one
                opQueue = new OperationQueue(ts);
                opQueues.add(opQueue);
            }
        }
        return opQueue.stepInAndWait(op);
    }


    /**
     * Current state for this calculator's key.
     */
    private static class State {

        Double lastResult = 0d;
    }

    /**
     * Class for keeping all information about specific calculator execution.
     */
    private static class Operation {

        Long ts;
        Map<String, Object> data;
        Short key;

        Operation(Short key, Long ts, Map<String, Object> data) {
            this.key = key;
            this.ts = ts;
            this.data = data;
        }
    }


    /**
     * Class for queueing operations
     */
    private class OperationQueue {
        final PriorityQueue<Operation> queue = new PriorityQueue<>((o1, o2) -> o1.ts > o2.ts ? 1 : -1);
        /**
         * Timestamp of this queue start. Any operation that has timestamp in range of (ts, ts + 100) will fit in this
         * queue.
         */
        Long ts;
        boolean open = true;
        /**
         * Map where threads can find results of operation execution by it's timestamp
         */
        private final Map<Long, Double> results = new ConcurrentHashMap<>();

        OperationQueue(Long ts) {
            this.ts = ts;
        }

        /**
         * Check if operation can be put in this queue
         */
        boolean isFitting(Operation op) {
            return isOpen() && ts <= op.ts;
        }

        /**
         * Is this queue still open for new operations
         */
        boolean isOpen() {
            return open;
        }

        /**
         * Put operation in this queue and wait for results calculation.
         * Results calculation will start after all fitting operations are put in this queue.
         */
        Double stepInAndWait(Operation op) throws Exception {
            boolean isFirst;
            synchronized (queue) {
                if (!open) {
                    System.out.println("Skipped calculation due to expiration. Key: " + key + "ts: " + op.ts);
                    return 0d;
                }
                isFirst = queue.isEmpty();
                queue.add(op);
            }
            if (isFirst) {//if this operation is first to enter this queue, it will execute the calculations once the time is right
                long waitTime = CALCULATION_STEP * 2 - (System.currentTimeMillis() - op.ts);
                waitTime = waitTime < 0 ? 0 : waitTime;
                Thread.sleep(waitTime);
                open = false;
                runCalculations();
            }
            Double result;
            synchronized (results) {
                result = results.get(op.ts);
                while (result == null) {//wait for result if not calculated yet
                    results.wait();
                    result = results.get(op.ts);
                }
            }
            return result;
        }

        /**
         * Run calculations for every operations in queue, ordered by their timestamp
         */
        private void runCalculations() throws Exception {
            synchronized (results) {
                synchronized (queue) {
                    System.out.println(Thread.currentThread().getId() + " Calculations started for key: " + key +
                            ". Operations in queue: " + queue.size() + " period ts: " + ts);
                    State current = stateCache;
                    Double result = current.lastResult;
                    while (!queue.isEmpty()) {
                        Operation op = queue.poll();
                        Map<String, Object> data = op.data;
                        for (Object o : data.values()) {//imitating order-dependent calculations
                            result = (result + 1) * o.hashCode();
                            result -= Math.floor(result / 1e5) * 1e5;
                        }
                        Thread.sleep(rand.nextInt(100));
                        results.put(op.ts, result);
                    }
                    current.lastResult = result;
                    results.notifyAll();
                    System.out.println("Calculations ended for key: " + key + " period ts: " + ts);
                }
            }
        }
    }
}

class Calculator {

    private static final int MAX_KEY = 1001;
    private static CalculatorForKey[] calculators = new CalculatorForKey[MAX_KEY];

    static {
        for (short i = 0; i < MAX_KEY; i++) {
            calculators[i] = new CalculatorForKey(i);
        }
    }

    /**
     * Run calculations for specific key. Since key varies from 1 to 1000, we can forward
     * call to specific key calculator, to separate state and execution of each key.
     */
    static Double calc(Short key, Long ts, Map<String, Object> data) throws Exception {
        return calculators[key].calc(ts, data);
    }
}