package com.atguigu.flink.homework;

import java.util.Arrays;

/**
 * 归并排序
 */
public class MergerSort {

    public static void main(String[] args) {

        int[] arr = {1, 3, 5, 7, 2, 4, 6, 8, 10};
        mergerSort(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }

    // 对数组中下标p~r的数据进行归并排序，最坏时间复杂度
    // T(N) = 2T(N/2) + O(N) = O(NlogN)
    public static void mergerSort(int[] arr, int left, int right) {

        // 递归的退出条件
        if (left < right) {
            // 中间下标
            int mid = (left + right) / 2;
            // 对left~mid归并排序
            mergerSort(arr, left, mid);
            // 对mid + 1~right归并排序
            mergerSort(arr, mid + 1, right);
            merger(arr, left, mid, right);
        }
    }

    public static void merger(int[] arr, int left, int mid, int right) {

        int n1 = mid - left + 1;
        int n2 = right - mid;

        // 每次归并两个数组时，都要分配新数组，占内存
        // 内存中的排序一般不用归并排序，使用快速排序
        int[] leftArr = new int[n1 + 1];
        int[] rightArr = new int[n2 + 1];

        for (int i = 0; i < n1; i++) leftArr[i] = arr[left + i];
        for (int j = 0; j < n2; j++) rightArr[j] = arr[mid + j + 1];

        leftArr[n1] = Integer.MAX_VALUE;
        rightArr[n2] = Integer.MAX_VALUE;

        // 将leftArr与rightArr归并
        int i = 0, j = 0;
        for (int k = left; k < right + 1; k++) {
            if (leftArr[i] <= rightArr[j]) {
                arr[k] = leftArr[i];
                i++;
            } else {
                arr[k] = rightArr[j];
                j++;
            }
        }
    }
}
