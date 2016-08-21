package com.calc;

import java.util.*;

public class Main {

    private static Random rand = new Random();

    public static void main(String[] args) throws Exception {
        Runnable r =() ->
        {
            try {
                while (true) {
                    short key = (short)(rand.nextInt(1000) + 1);
                    Long ts = System.currentTimeMillis();
                    Map<String, Object> data = new HashMap<>();
                    for (Integer i = 0; i < 10; i++)
                        data.put(i.toString(), new Object());//just creating objects to use their hashes in calculations
                    Double result = Calculator.calc(key, ts, data);
                    System.out.println(Thread.currentThread().getId() + " key: " + key + " ts: " + ts + " calculated: " + result);
                    Thread.sleep(rand.nextInt(15) + 5);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        for(int i = 0; i < 10; i++)
        {
            new Thread(r).start();
            Thread.sleep(20);
        }

    }
}
