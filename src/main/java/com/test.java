package com;

import org.apache.flink.types.Row;

import java.util.Arrays;

public class test {
    public static void main(String[] args) {
        final Row row = new Row(1);
        System.out.println(row);
        final String[] strings = new String[10];
        System.out.println(strings);
        System.out.println(Arrays.toString(strings));
    }
}
