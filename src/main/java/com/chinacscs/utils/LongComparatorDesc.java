package com.chinacscs.utils;

import java.util.Comparator;

/**
 * @author wangjj
 * @date 2016/12/30 17:25
 * @description Long类型逆序排序
 * @copyright(c) chinacscs all rights reserved
 */
public class LongComparatorDesc implements Comparator<Long> {
    @Override
    public int compare(Long o1, Long o2) {
        if(o2 > o1) {
            return 1;
        } else if(o2 < o1) {
            return -1;
        } else {
            return 0;
        }
    }
}
