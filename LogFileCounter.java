/*
 * First naive version of http://pastebin.com/f968c762 but in java.  
 */

package com.thyberg.logparser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class LogFileCounter {
    private static final int WORKERS = 20;

    public static void main(final String[] args) throws IOException {
        final Reader reader = new Reader("/usr/share/dict/words");
        final List<Summer> workers = new ArrayList<Summer>(WORKERS);

        final Runnable merger = new Runnable() {
                public void run() {
                    final Comparator<Entry<String, Integer>> c =
                        new Comparator<Entry<String, Integer>>() {
                            @Override
                            public int compare(
                                final Entry<String, Integer> o1,
                                final Entry<String, Integer> o2
                            ) {
                                return o2.getValue() - o1.getValue();
                            }
                        };

                    final Map<String, Integer> map =
                        new HashMap<String, Integer>();

                    for (final Summer s : workers) {
                        for (
                            final Entry<String, Integer> e :
                            s.getMap()
                            .entrySet()
                        ) {
                            Integer count = map.get(e.getKey());

                            if (count == null) {
                                count = 0;
                                map.put(e.getKey(), count);
                            }
                            count += e.getValue();
                            map.put(e.getKey(), count);
                        }
                    }

                    final List<Entry<String, Integer>> list =
                        new LinkedList<Entry<String, Integer>>();
                    list.addAll(map.entrySet());
                    Collections.sort(list, c);

                    int i = 0;

                    for (final Entry<String, Integer> e : list) {
                        System.out.println(e.getKey() + " " + e.getValue());

                        if (++i >= 10) {
                            break;
                        }
                    }
                }
            };

        final CyclicBarrier barrier = new CyclicBarrier(WORKERS, merger);

        for (int i = 0; i < WORKERS; i++) {
            final Summer worker = new Summer(barrier, reader);
            workers.add(worker);
            worker.start();
        }
    }

    private static class Reader extends Thread {
        final BufferedReader reader;

        public Reader(final String filename) throws FileNotFoundException {
            reader = new BufferedReader(new FileReader(filename));
        }

        public synchronized String getLine() throws IOException {
            return reader.readLine();
        }
    }

    private static class Summer extends Thread {
        final CyclicBarrier barrier;
        final Reader reader;
        final Map<String, Integer> map = new HashMap<String, Integer>();

        public Summer(final CyclicBarrier barrier, final Reader reader) {
            this.barrier = barrier;
            this.reader = reader;
        }

        public Map<String, Integer> getMap() {
            return this.map;
        }

        @Override
        public void run() {
            String line = null;

            try {
                while ((line = reader.getLine()) != null) {
                    final String key = extract(line);
                    if (key == null) {
                        continue;
                    }
                    Integer count = map.get(key);
                    if (count == null) {
                        count = 0;
                        map.put(key, count);
                    }
                    ++count;
                    map.put(key, count);

                }
            }
            catch (final IOException e) {
                e.printStackTrace();
            }

            try {
                barrier.await();
            }
            catch (final InterruptedException e) {
                e.printStackTrace();
            }
            catch (final BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        private String extract(final String line) {

            if (line.length() >= 5) {
                return line.substring(2, 5);
            }
            return null;
        }
    }
}
