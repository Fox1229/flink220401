package com.atguigu.flink.wc;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 有向无环图
 *
 * {
 *     "source", ["flatMap[0]", "flatMap[1]"],
 *     "flatMap[0]", ["source[0]", source[1]],
 *     "flatMap[1]", ["source[0]", source[1]],
 *     "source[0]", ["print"],
 *     "source[1]", ["print"]
 * }
 */
public class DAG {

    public static void main(String[] args) {

        HashMap<String, ArrayList<String>> dag = new HashMap<>();

        ArrayList<String> flatMap = new ArrayList<>();
        flatMap.add("flatMap[0]");
        flatMap.add("flatMap[1]");
        dag.put("source", flatMap);

        ArrayList<String> flatMap0Nei = new ArrayList<>();
        flatMap0Nei.add("reduce[0]");
        flatMap0Nei.add("reduce[1]");
        dag.put("flatMap[0]", flatMap0Nei);
        dag.put("flatMap[1]", flatMap0Nei);

        ArrayList<String> reduce0Nei = new ArrayList<>();
        reduce0Nei.add("print");
        dag.put("reduce[0]", reduce0Nei);
        dag.put("reduce[1]", reduce0Nei);

        topologySort(dag, "source", "source");
    }

    // 执行流程
    // topologySort(dag, "source", "source")
    // topologySort(dag, "flatMap[0]", "source -> flatMap[0]")
    // topologySort(dag, "reduce[0]", "source -> flatMap[0] -> reduce[0]")
    // topologySort(dag, "print", "source -> flatMap[0] -> source[0] -> print")
    public static void topologySort(HashMap<String, ArrayList<String>> map, String vertex, String result) {

        if("print".equals(vertex)) {
            // 遍历到终点，输出结果
            System.out.println(result);
        } else {
            // 获取下一级节点
            ArrayList<String> neiList = map.get(vertex);
            for (String next : neiList) {
                topologySort(map, next, result + " -> " + next);
            }
        }
    }
}
