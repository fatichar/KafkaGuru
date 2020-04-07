package com.loco.kafkaguru.viewmodel;

import lombok.Data;
import org.apache.kafka.common.PartitionInfo;

import javax.swing.tree.TreeNode;
import java.util.Enumeration;

@Data
public class PartitionNode implements AbstractNode {
    private AbstractNode parent;
    private PartitionInfo partition;
    private final String name;

    public PartitionNode(AbstractNode parent, PartitionInfo partition) {
        this.parent = parent;
        this.partition = partition;
        name = "Partition " + partition.partition();
    }

    public String toString(){
        return name;
    }

    public boolean equals(Object other){
        PartitionNode otherNode = (PartitionNode) other;
        if (otherNode  == null){
            return false;
        }
        if (partition != otherNode.getPartition()){
            return false;
        }
        return true;
    }

    public int hashCode(){
        return toString().hashCode();
    }
}
