package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 基于行政区、分类编码的并发查询任务分配
 */
public class RegionCategoryBasedScanTaskSpliter implements IScanTaskSpliter{

    private String regionCode;      //行政区代码
    private String categoryCode;    //分类编码
    private Filter filter;          //过滤器


    public RegionCategoryBasedScanTaskSpliter(String regionCode, String categoryCode,Filter filter) {
        super();
        this.regionCode = RegionManager.GetStandardRegionCode(regionCode);
        this.filter = filter;
        this.categoryCode = categoryCode;
    }



    @Override
    public List<Scan> GetSplitTasks() {
        // TODO Auto-generated method stub

        //查询全部类别,按照基于行政区的扫描
        if(categoryCode.equals(CategoryManager.TOPCC)){
            RegionBasedScanTaskSpliter regionBasedScanTaskSpliter = new RegionBasedScanTaskSpliter(regionCode, filter);
            return regionBasedScanTaskSpliter.GetSplitTasks();
        }

        List<String> countyList = RegionManager.GetCountyList(regionCode);
        List<Scan> scanList = new ArrayList<Scan>(countyList.size());
        for(String county:countyList){
            String[] range = new String[2];
            range[0] = Utils.GetROWCODE(county, categoryCode, "00000000");
            range[1] = Utils.GetROWCODE(county, CategoryManager.getUpperCategory(categoryCode), "00000000");
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(range[0]));
            scan.setStopRow(Bytes.toBytes(range[1]));
            if(filter!=null)
                scan.setFilter(filter);
            scanList.add(scan);
        }

        //添加未分区数据搜索,暂时这样写吧
        if(regionCode.equals(RegionManager.provinceCode)){
            String[] range = new String[2];
            range[0] = Utils.GetROWCODE(RegionManager.provinceCode+"000000", categoryCode, "00000000");
            range[1] = Utils.GetROWCODE(RegionManager.upperProvinceCode+"000000", CategoryManager.getUpperCategory(categoryCode), "00000000");
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(range[0]));
            scan.setStopRow(Bytes.toBytes(range[1]));
            if(filter!=null)
                scan.setFilter(filter);
            scanList.add(scan);
        }

        return scanList;
    }



}
