package com.atguigu.flink.homework;

import java.util.Arrays;

public class MergerSort2 {

    public static void main(String[] args) {

        int[] arr = {1, 3, 5, 7, 2, 4, 6, 8, 10};
        mergerSort(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }

    public static void mergerSort(int[] arr, int left, int right) {

        if (left < right) {
            int mid = (left + right) / 2;
            mergerSort(arr, left, mid);
            mergerSort(arr, mid + 1, right);
            merger(arr, left, mid, right);
        }
    }

    public static void merger(int[] arr, int left, int mid, int right) {

        int n1 = mid - left + 1;
        int n2 = right - mid;

        int[] leftArr = new int[n1 + 1];
        int[] rightArr = new int[n2 + 1];

        for (int i = 0; i < n1; i++) leftArr[i] = arr[left + i];
        for (int j = 0; j < n2; j++) rightArr[j] = arr[mid + j + 1];

        leftArr[n1] = Integer.MAX_VALUE;
        rightArr[n2] = Integer.MAX_VALUE;

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
