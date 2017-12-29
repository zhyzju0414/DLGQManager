package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 基于行政区的并发查询任务分配
 */
public class RegionBasedScanTaskSpliter implements IScanTaskSpliter{

    private String regionCode;      //行政区代码
    private Filter filter;          //过滤器


    public RegionBasedScanTaskSpliter(String regionCode, Filter filter) {
        super();
        this.regionCode = regionCode;
        this.filter = filter;
    }



    @Override
    public List<Scan> GetSplitTasks() {
        // TODO Auto-generated method stub
        List<String> countyList = RegionManager.GetCountyList(regionCode);
        List<Scan> scanList = new ArrayList<Scan>(countyList.size());
        for(String county:countyList){
//			if(RegionManager.IsProvinceCode(county)){
//				scanList.clear();
//
//			}
            String[] range = RegionManager.GetRowkeyRange(county);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(range[0]));
            scan.setStopRow(Bytes.toBytes(range[1]));
            if(filter!=null)
                scan.setFilter(filter);
            //如果countyList里面有一个county是省的话 就直接清除之前的scanList 把这个省的scan范围加入到scanList里面
            //然后后面的county也不需要再进行判断  直接扫描全省
            //这个情况是否只适用于要进行全国范围内的查询统计？
            //如果是查询整个省  还是要把整个省拆分成县 然后用多线程来计算
            if(RegionManager.IsProvinceCode(county)){   //2016 03 22 zxw
                scanList.clear();
                scanList.add(scan);
                return scanList;
            }
            scanList.add(scan);
        }
        return scanList;
    }

}
