package com.intel.distml.platform;

import java.util.*;

/**
 * Created by yunlong on 16-4-7.
 */
public class SSP {

    class Worker {
        int index;
        int progress;
        boolean waiting;

        public Worker(int index, int progress) {
            this.index = index;
            this.progress = progress;
            waiting = false;
        }
    }

    class SortHelper implements Comparator<Worker> {

        public int compare(Worker o1, Worker o2) {
            return o2.progress - o1.progress;
        }
    }


    public class CheckResult {
        public int index;
        public int progress;
        public boolean waiting;
        public LinkedList<Integer> workersToNotify;

        public CheckResult(int index, int progress) {
            this.index = index;
            this.progress = progress;
            waiting = false;

        }

    }

    LinkedList<Worker> workers;
    int maxIterations;
    int maxLag;
    SortHelper sorter = new SortHelper();

    public SSP(int maxIterations, int maxLag) {
        workers = new LinkedList<Worker>();
        this.maxIterations = maxIterations;
        this.maxLag = maxLag;
    }

    /**
     * set progress and check whether to wait
     *
     *
     * @param worker
     * @param iter
     * @return
     */
    public CheckResult progress(int worker, int iter) {

        CheckResult result = new CheckResult(worker, iter);
        Worker w = null;

        boolean found = false;
        for (int i = 0; i < workers.size(); i++) {
            w = workers.get(i);
            if (w.index == worker) {
                assert (w.progress == iter - 1);
                w.progress++;
                found = true;
                break;
            }
        }
        if (!found)
            workers.add(new Worker(worker, iter));

        Collections.sort(workers, sorter);

        Worker last = workers.getLast();
        if (worker != last.index) {
            if (iter - last.progress >= maxLag) {
                result.waiting = true;
                w.waiting = true;
                //System.out.println("worker " + worker + " is waiting for " + last.index);
            }
        }
        result.workersToNotify = workersToNotify(w);

        //show();

        return result;
    }

    public LinkedList<Integer> workersToNotify(Worker p) {
        Iterator<Worker> it = workers.iterator();
        Worker last = workers.getLast();

        LinkedList<Integer> workersToNotify = new LinkedList<Integer>();
        while(it.hasNext()) {
            Worker w = it.next();
            if (w == p) break;

            //System.out.println("check notify: " + w.progress + ", " + last.progress);
            if (w.progress - last.progress < maxLag) {
                if (w.waiting) {
                    workersToNotify.add(w.index);
                    w.waiting = false;
                }
            }
        }

        return workersToNotify;
    }

    private void show() {
        for (int i = 0;i < workers.size(); i++) {
            Worker w = workers.get(i);
            System.out.print("w[" + i + "]=" + w.progress + "\t");
        }
        System.out.println();
    }

}
