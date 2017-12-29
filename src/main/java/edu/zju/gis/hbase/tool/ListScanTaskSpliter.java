package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

public class ListScanTaskSpliter implements IScanTaskSpliter {

    List<Scan> totalScanList;  //总的Scan数


    public ListScanTaskSpliter(List<Scan> totalScanList) {
        super();
        this.totalScanList = totalScanList;
    }

    @Override
    public List<Scan> GetSplitTasks() {
        // TODO Auto-generated method stub
        return totalScanList;
    }

}
