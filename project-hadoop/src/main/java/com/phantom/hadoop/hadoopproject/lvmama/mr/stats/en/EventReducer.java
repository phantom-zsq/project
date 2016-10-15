package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.phantom.hadoop.hadoopproject.lvmama.common.KpiType;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.EventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsEventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;

/**
 * reducer类
 * 
 * @author ibf
 *
 */
public class EventReducer
        extends Reducer<StatsEventDimension, NullWritable, StatsEventDimension, MapWritableValue> {
    private List<Node> forest = new ArrayList<Node>(); // 森林
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable mapWritable = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mapWritable = outputValue.getValue();
        outputValue.setKpi(KpiType.EVENT_TIMES);
    }

    @SuppressWarnings("unused")
    @Override
    protected void reduce(StatsEventDimension key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        // begin
        this.forest.clear(); // 清空集合
        String preSessionId = null; // 上一条有效数据的会话id
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();

        for (NullWritable value : values) {
            // 每次循环，二次排序中，key的某些属性会进行变化
            EventDimension event = key.getEvent();
            String currentSessionId = key.getSessionId(); // 当前记录的sessionid
            int originId = event.getOriginId();
            Node node = new Node();
            node.id = event.getId();
            node.hasMultiCharldren = 1 == event.getHasMultChidren(); // 值为1的时候表示允许存在

            if (preSessionId == null) {
                if (originId == 0) {
                    preSessionId = currentSessionId; // 新的会话的产生
                    this.forest.add(node);
                } else {
                    // TODO: 自己添加日志
                }
            } else if (preSessionId.equals(currentSessionId)) {
                // 属于同一个会话
                if (originId == 0) {
                    // 表示一个新的数据流从开始
                    this.forest.add(node);
                } else {
                    // 添加到森林中，如果在森林中没有找到对于的父节点，不进行添加
                    this.insertNodeToForest(node, originId);
                }
            } else {
                // 属于一个新的会话
                // 1、计算上一个会话的事件触发次数吧
                Map<Integer, Integer> tmpResult = this.calcEventTimes();
                
                for (Map.Entry<Integer, Integer> entry : tmpResult.entrySet()) {
                    Integer k = entry.getKey();
                    Integer v = entry.getValue();

                    if (result.containsKey(k)) {
                        v += result.get(k); // 累加
                    }
                    result.put(k, v);
                }

                // 2、开始一个新的会话
                this.forest.clear(); // 清空上一个会话的访问记录
                if (event.getOriginId() == 0) {
                    preSessionId = currentSessionId; // 新的会话的产生
                    this.forest.add(node);
                } else {
                    preSessionId = null; // 表示当前记录不是事件流的开始
                }
            }
        }

        // 针对最后一个会话中的数据进行统计
        Map<Integer, Integer> tmpResult = this.calcEventTimes();
        for (Map.Entry<Integer, Integer> entry : tmpResult.entrySet()) {
            Integer k = entry.getKey();
            Integer v = entry.getValue();

            if (result.containsKey(k)) {
                v += result.get(k); // 累加
            }
            result.put(k, v);
        }

        // 结果输出
        for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
            key.getEvent().setId(entry.getKey()); // 修改维度id
            this.mapWritable.put(new IntWritable(-1), new IntWritable(entry.getValue()));
            context.write(key, outputValue);
        }
        // end
    }

    /**
     * 计算结果
     * 
     * @return
     */
    private Map<Integer, Integer> calcEventTimes() {
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();

        for (Node tree : this.forest) {
            Map<Integer, Integer> tmpResult = this.calcTreeEventTimes(tree);
            for (Map.Entry<Integer, Integer> entry : tmpResult.entrySet()) {
                Integer k = entry.getKey();
                Integer v = entry.getValue();
                if (result.containsKey(k)) {
                    v += result.get(k); // 结果累加
                }
                result.put(k, v);
            }
        }
        return result;
    }

    /**
     * 计算对应树中各个事件的触发次数
     * 
     * @param tree
     * @return
     */
    private Map<Integer, Integer> calcTreeEventTimes(Node tree) {
        Map<Integer, Integer> result = new HashMap<Integer, Integer>();

        if (tree.charldrens.isEmpty()) {
            // 是叶子节点吧，没有子节点
            result.put(tree.id, 1);
        } else {
            // 非叶子节点，他触发次数是叶子节点触发次数的总和
            int currentNoeTimes = 0;
            for (Node node : tree.charldrens) {
                Map<Integer, Integer> tmpResult = this.calcTreeEventTimes(node);
                for (Map.Entry<Integer, Integer> entry : tmpResult.entrySet()) {
                    Integer k = entry.getKey();
                    Integer v = entry.getValue();
                    if (result.containsKey(k)) {
                        v += result.get(k);
                    }
                    result.put(k, v);
                }
                currentNoeTimes += tmpResult.get(node.id); // 添加子节点的触发次数
            }
            result.put(tree.id, currentNoeTimes);
        }

        return result;
    }

    /**
     * 添加节点到森林中<br/>
     * 构造树
     * 
     * @param node
     */
    private void insertNodeToForest(Node node, int originId) {
        for (Node tree : this.forest) {
            // 寻找到一个可以插入子节点的父节点
            Node parentNode = this.fetchParentNodeByOriginId(tree, originId);
            if (parentNode != null) {
                // 表示找到到了父节点，进行串联起来
                parentNode.charldrens.add(node); // 添加到子列表中
                if (!parentNode.hasMultiCharldren) {
                    // 不允许多个子节点
                    parentNode.allowInsert = false; // 该节点不允许基础插入子节点了
                }
                break; // 结束操作
            }
        }
    }

    /**
     * 获取有效的父节点
     * 
     * @param tree
     * @param originId
     * @return
     */
    private Node fetchParentNodeByOriginId(Node tree, int originId) {
        Node result = null;

        if (tree.id == originId) {
            if (tree.allowInsert) {
                result = tree;
            } else {
                // 直接返回，表示这颗树找不到对应的数据
            }
        } else {
            // 从tree的子节点中寻找对于的结果
            for (Node node : tree.charldrens) {
                Node tmpResult = this.fetchParentNodeByOriginId(node, originId);
                if (tmpResult != null) {
                    result = tmpResult;
                    break; // 跳出循环
                }
            }
        }
        return result;
    }

    public static class Node {
        public int id; // event的维度id
        public boolean hasMultiCharldren; // 是否允许有多个子节点
        public boolean allowInsert = true; // 允许插入数据
        public List<Node> charldrens = new ArrayList<Node>(); // 子节点链表
    }
}
