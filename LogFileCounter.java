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
import java.util.concurrent.CyclicBarrier;


public class LogFileCounter {
    private static final int PRINT_ITEMS = 10;
    private static final int WORKERS = 20;

    public static void main(final String[] args) throws IOException {
        final Reader reader = new Reader("/usr/share/dict/words");
        final List<Summer> workers = new ArrayList<Summer>(WORKERS);
        final CyclicBarrier barrier = new CyclicBarrier(
            WORKERS,
            new Merger(workers)
        );

        for (int i = 0; i < WORKERS; i++) {
            final Summer worker = new Summer(barrier, reader);
            workers.add(worker);
            worker.start();
        }
    }

    private static final class Merger implements Runnable {
        private final List<Summer> workers;

        public Merger(final List<Summer> workers) {
            this.workers = workers;
        }

        public void run() {
            final Map<String, Integer> map = new HashMap<String, Integer>();

            for (final Summer s : workers) {
                for (final Entry<String, Integer> e : s.getMap()
                    .entrySet()
                ) {
                    map(map, e.getKey(), e.getValue());
                }
            }
            printList(sortMap(map));
        }

        private void printList(final List<Entry<String, Integer>> list) {
            for (int i = 0; i < PRINT_ITEMS; ++i) {
                final Entry<String, Integer> item = list.get(i);
                System.out.println(item.getKey() + " " + item.getValue());
            }
        }

        private void map(
            final Map<String, Integer> map,
            final String key,
            final int value
        ) {
            Integer count = map.get(key);

            if (count == null) {
                count = 0;
            }
            count += value;
            map.put(key, count);
        }

        private List<Entry<String, Integer>> sortMap(
            final Map<String, Integer> map
        ) {
            final LinkedList<Entry<String, Integer>> list =
                new LinkedList<Entry<String, Integer>>();
            list.addAll(map.entrySet());
            Collections.sort(
                list,
                new Comparator<Entry<String, Integer>>() {
                    @Override
                    public int compare(
                        final Entry<String, Integer> o1,
                        final Entry<String, Integer> o2
                    ) {
                        return o2.getValue() - o1.getValue();
                    }
                }
            );
            return list;
        }
    }

    private static class Reader extends Thread {
        private final BufferedReader reader;

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

        private void addLine(final String key) {
            if (key == null) {
                return;
            }

            Integer count = map.get(key);

            if (count == null) {
                count = 0;
            }
            ++count;
            map.put(key, count);
        }

        @Override
        public void run() {
            String line = null;

            try {
                while ((line = reader.getLine()) != null) {
                    addLine(extract(line));
                }
            }
            catch (final IOException e) {
                e.printStackTrace();
            }

            try {
                barrier.await();
            }
            catch (final Exception e) {
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
