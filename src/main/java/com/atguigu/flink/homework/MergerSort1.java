package com.atguigu.flink.homework;

import java.util.Arrays;

public class MergerSort1 {

    public static void main(String[] args) {

        int[] arr = {1, 3, 5, 7, 2, 4, 6, 8, 10};
        int[] tmp = new int[arr.length];
        mergerSort(arr, 0, arr.length - 1, tmp);
        System.out.println(Arrays.toString(arr));
    }

    //分解方法
    public static void mergerSort(int[] arr, int left, int right, int[] temp) {

        if (left < right) {
            int mid = (left + right) / 2; //中间索引
            //向左递归进行分解
            //0 - mid 即是0 - 3 {8,4,5,7}
            mergerSort(arr, left, mid, temp);
            //向右递归进行分解
            //mid+1 - midright 即是4 - 7  {1,3,6,2}
            mergerSort(arr, mid + 1, right, temp);
            //进行合并
            merger(arr,left,mid,right,temp);
        }
    }

    //合并的方法
    /**
     *
     *  @param  arr 排序的原始数组
     *  @param left 左边有序序列的初始索引
     *  @param mid 中间索引
     *  @param right 右边索引
     *  @param temp 做中转的数组
     **/
    public static void merger(int[] arr, int left, int mid, int right, int[] temp) {

        //初始化i,左边有序序列的初始索引
        int i = left;
        //初始化j,右边有序序列的初始索引
        //为什么要mid+1?
        //因为假设数组arr{1,3,5,6,2,4} mid=(left+right)/2 = 2
        //此时左边i=left mid左边的就是 0 - mid 即是{1,3,5}
        //此时右边就是mid+1 - right 即是{6,2,4}
        int j = mid+1;

        int t = 0;//指向temp数组的当前索引

        //(-)
        //先把左右两边(有序)的数据按照规则填充到temp数组
        //直到左右两边的有序序列，有一边处理完毕为止
        //i <= mid 代表左边有序序列有值
        //j <= right 代表右边有序序列有值
        while (i <= mid & j <= right) {//继续

            //如果左边的有序序列的当前元素，小于等于右边有序序列的当前元素
            //即将左边的当前元素，拷贝到temp数组
            //假设数组arr{1,3,5,6,2,4}
            //左边 0 - mid 即是{1,3,5}
            //右边 mid+1 -right 即是{6,2,4}
            //若arr[i]<= arr[j] 即是1 <= 6
            if (arr[i] <= arr[j]) {
                temp[t] = arr[i];//temp[0]=arr[i];
                t += 1;//指向temp数组下一位
                i += 1;//指向左边下一位arr[i+1]...
            }else{
                //反之arr[i] >= arr[j] 左边大于右边
                //则进行右边赋值给temp数组
                temp[t] = arr[j];//temp[0]=arr[i];
                t += 1;//指向temp数组下一位
                j += 1;//指向右边边下一位arr[j+1]...
            }
        }

        //(二)
        //把有剩余数据的一边的数据依次全部填充到temp
        //左边的有序序列还有剩余的元素，就全部填充到temp
        while( i <= mid){
            temp[t] = arr[i];
            t += 1;
            i += 1;
        }
        //右边的有序序列还有剩余的元素，就全部填充到temp
        while( j <= right){
            temp[t] = arr[j];
            t += 1;
            j += 1;
        }

        //(三)
        //将temp数组的元素拷贝到arr
        //为什么 t=0 ?
        //因为合并的时候按图所示数组：{8,4,5,7,1,3,6,2}
        //最先进入的是84 left=0 right = 1
        //经过上面的左边与右边比较，得出temp数组：4,8
        // 此时清空指向temp数组的下标指针t 重新回到0
        //tempLeft = 0 进行将temp数组里的4，8 赋值给arr数组
        t = 0;
        int tempLeft= left;
        while( tempLeft <= right){
            arr[tempLeft]=temp[t];
            t += 1;//赋值成功后指向temp数组的下标指针t往后移
            tempLeft +=1;//84 完成后到57 此时left=2 right = 3 ...
        }
    }

}
