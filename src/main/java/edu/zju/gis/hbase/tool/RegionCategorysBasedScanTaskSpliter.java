package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.query.querycondition.PropertyQueryCondition;

/*
 * 基于行政区、分类编码的并发查询任务分配,支持多个分类编码的并发查询
 */
public class RegionCategorysBasedScanTaskSpliter implements IScanTaskSpliter{

    private String regionCode;      //行政区代码
    private String[] categoryCodes;    //分类编码
    private Filter filter;          //过滤器


    public RegionCategorysBasedScanTaskSpliter(String regionCode, String[] categoryCodes,Filter filter) {
        super();
        this.regionCode = RegionManager.GetStandardRegionCode(regionCode);
        this.filter = filter;

        //需要对分类编码有一个合并操作，有包含关系的分类需要进行合并

        this.categoryCodes = categoryCodes;
    }



    @Override
    public List<Scan> GetSplitTasks() {
        // TODO Auto-generated method stub
        List<Scan> scanList = new ArrayList<Scan>();
        List<String> countyList = RegionManager.GetCountyList(regionCode);

        for(int i=0;i<categoryCodes.length;i++){
            //查询全部类别,按照基于行政区的扫描
            if(categoryCodes[i].equals(CategoryManager.TOPCC)){
                RegionBasedScanTaskSpliter regionBasedScanTaskSpliter = new RegionBasedScanTaskSpliter(regionCode, filter);
                return regionBasedScanTaskSpliter.GetSplitTasks();
            }

            for(String county:countyList){
                String[] range = new String[2];
                range[0] = Utils.GetROWCODE(county, categoryCodes[i], "00000000");
                range[1] = Utils.GetROWCODE(county, CategoryManager.getUpperCategory(categoryCodes[i]), "00000000");
                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes(range[0]));
                scan.setStopRow(Bytes.toBytes(range[1]));
                if(filter!=null)
                    scan.setFilter(filter);
                scanList.add(scan);
            }

            //添加未分区数据搜索,暂时这样写吧
//			if(regionCode.equals(RegionManager.provinceCode)){
//				String[] range = new String[2];
//				range[0] = Utils.GetROWCODE(RegionManager.provinceCode+"000000", categoryCodes[i], "00000000");
//				range[1] = Utils.GetROWCODE(RegionManager.provinceCode+"000000", CategoryManager.getUpperCategory(categoryCodes[i]), "00000000");
//				Scan scan = new Scan();
//				scan.setStartRow(Bytes.toBytes(range[0]));
//				scan.setStopRow(Bytes.toBytes(range[1]));
//				if(filter!=null)
//				 scan.setFilter(filter);
//				scanList.add(scan);
//			}
        }

        return scanList;
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        PropertyQueryCondition pqc = new PropertyQueryCondition(new String[]{"0000"},"330100000000");
        HTablePool dataTablePool = new HTablePool(3, conf, "LCRA1");
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Utils.COLUMNFAMILY_STATINDEX));
        IScanTaskSpliter hgs = new RegionCategorysBasedScanTaskSpliter(pqc.getRegionCode(),pqc.getCategoryCode(),filter);
        IResultProcessor rp = new RawResultProcessor();
        BatchScanServer bgs = new BatchScanServer<Result>(hgs, dataTablePool, rp);
        List<Result> result = result = bgs.ParallelBatchScan();
        int count = 0;
        for(int i=0;i<result.size();i++){
            count++;
        }
        System.out.println(count);
    }

    public static void testGetSplitTasks(){
        String region = "33";
        String[] cats = new String[]{"1114"};
        PropertyQueryCondition qcon = new PropertyQueryCondition(cats, region);

        IScanTaskSpliter hgs = new RegionCategorysBasedScanTaskSpliter(qcon.getRegionCode(),qcon.getCategoryCode(),null);
        List<Scan> scanList = hgs.GetSplitTasks();
    }




}
