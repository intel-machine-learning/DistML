package com.intel.distml.example;


import java.util.*;

/**
 * Created by yunlong on 12/28/15.
 */
public class TestJava {

    public static final void main(String[] args) {
        HashMap<Long, Integer> dt = new HashMap<Long, Integer>();
        dt.put(0L, 10);
        int i = 1;
        dt.put((long)i, 10);
        System.out.println("value: " + dt.get(0L));
    }

}
