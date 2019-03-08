package com.test.sheldon;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        int comFirst = Integer.valueOf(this.first).compareTo(o.first);
        if (comFirst != 0) {
            return comFirst;
        } else {
            return Integer.valueOf(o.second).compareTo(this.second);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.first);
        dataOutput.writeInt(this.second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readInt();
        this.second = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return this.first * 163 + this.second;
    }

    @Override
    public String toString() {
        return this.first + "\t" + this.second;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntPair) {
            IntPair ip = (IntPair) obj;
            return first == ip.getFirst() && this.second == ip.second;
        }
        return false;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public static int compare(int first1, int first2) {
        return Integer.valueOf(first1).compareTo(Integer.valueOf(first2));
    }
}
