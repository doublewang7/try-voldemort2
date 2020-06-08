package com.cisco.wap.utils.tuple;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Tuple2<T1, T2> {
    private T1 t1;
    private T2 t2;

    public T1 _1() {
        return t1;
    }

    public T2 _2() {
        return t2;
    }

}
