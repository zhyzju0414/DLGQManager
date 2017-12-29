package edu.zju.gis.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticRequest;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse.Builder;
import edu.zju.gis.hbase.coprocessor.CategoryStatisticProtos.CategoryStatisticResponse.CategoryStatisticInfo;





public class CategoryStatisticEndPoint extends CategoryStatisticProtos.CategoryStatisticService
        implements Coprocessor, CoprocessorService {

    public CategoryStatisticEndPoint() {

    }

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        // TODO Auto-generated method stub
        return this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // TODO Auto-generated method stub
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    /***********************辅助函数,开始***************************/
    private static String GetCategoryCode(String rowcode){
        return rowcode.substring(12, 16);
    }


    private static String getUpperCategory(String categoryCode){
        int offset = categoryCode.length();
        while(categoryCode.charAt(offset-1)=='0'){
            offset--;
        }
        String sub = categoryCode.substring(0,offset);
        if(sub.charAt(0)=='0'){
            sub = sub.replaceFirst("0", "1");
            String upperCategory = Integer.toString(Integer.parseInt(sub.substring(0,offset))+1);
            if(upperCategory.charAt(0)=='2'){
                upperCategory = upperCategory.replaceFirst("2", "1");
            }else{
                upperCategory = upperCategory.replaceFirst("1", "0");
            }
            return padRightStr(upperCategory, 4);
        }else{
            String upperCategory = Integer.toString(Integer.parseInt(sub.substring(0,offset))+1);
            return padRightStr(upperCategory, 4);
        }
    }


    private static String padLeftStr(String res, int pad) {
        if (pad > 0) {
            while (res.length() < pad) {
                res = "0" + res;
            }
        }
        return res;
    }

    private static String padRightStr(String res, int pad) {
        if (pad > 0) {
            while (res.length() < pad) {
                res = res + "0";
            }
        }
        return res;
    }

    private static String trimEnd(String str,char c){
        int endindex = str.length()-1;
        while(endindex>=0&&str.charAt(endindex)==c){
            endindex--;
        }
        return str.substring(0, endindex+1);
    }
    /***********************辅助函数，结束***************************/
    @Override
    public void getStatisticInfo(RpcController controller,
                                 CategoryStatisticRequest request,
                                 RpcCallback<CategoryStatisticResponse> done) {
        // TODO Auto-generated method stub
        try {

            //收集统计结果
            Map<String,edu.zju.gis.hbase.entity.StatisticInfo> statisticResult = new HashMap<String,edu.zju.gis.hbase.entity.StatisticInfo>();

            List<Filter> filterList = new ArrayList<Filter>();
            //trimend的作用是把后面的0给去掉
            String regionCode =trimEnd(request.getRegioncode(),'0');
            if(regionCode.length() == 3||regionCode.length() == 5){
                //331000 或者330110
                regionCode = regionCode+"0";
            }
            for(int i=0;i<request.getCategorycodeCount();i++){
                String queryCC = trimEnd(request.getCategorycode(i),'0');
                if(!(queryCC.length()==0)){
                    String reg = String.format("\\d{12}%s\\d{%d}\\d{8}",queryCC,4-queryCC.length());
                    Filter rowfilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new RegexStringComparator(reg));
                    filterList.add(rowfilter);
                }
            }
            Scan scan = new Scan();
            Filter columnfilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("CFStatIndex")));
            if(filterList.size()!=0){
                Filter rowFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE,filterList);
                scan.setFilter(new FilterList(rowFilter,columnfilter));
            }else{
                scan.setFilter(columnfilter);
            }

            List<Cell> results = new ArrayList<Cell>();
            InternalScanner scanner = env.getRegion().getScanner(scan);

            boolean hasMore = false;
            byte[] lastRow = null;
            do {
                hasMore = scanner.next(results);

                if(results.size()>0){
                    String rowkey = Bytes.toString(results.get(0).getRowArray(),results.get(0).getRowOffset(),results.get(0).getRowLength());
                    if(rowkey.startsWith(regionCode)){
                        String cc = GetCategoryCode(rowkey);
                        if(!statisticResult.containsKey(cc)){
                            statisticResult.put(cc, new edu.zju.gis.hbase.entity.StatisticInfo());
                        }

                        for (Cell cell : results) {
                            byte[] qualifier = cell.getQualifier();
                            if(Bytes.equals(qualifier,Bytes.toBytes("AREA"))){
                                statisticResult.get(cc).AREA+=Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                            }else if(Bytes.equals(qualifier,Bytes.toBytes("LENGTH"))){
                                statisticResult.get(cc).LENGTH+=Double.parseDouble(Bytes.toString(cell.getValueArray(),cell.getValueOffset(), cell.getValueLength()));
                            }
                        }
                        statisticResult.get(cc).COUNT++;
                    }
                    results.clear();
                }

            } while (hasMore);

            Builder responseBuilder = CategoryStatisticResponse.newBuilder();

            for(Map.Entry<String,edu.zju.gis.hbase.entity.StatisticInfo> entry:statisticResult.entrySet()){
                edu.zju.gis.hbase.entity.StatisticInfo statisticinfo = entry.getValue();
                responseBuilder.addCategoryStatisticInfo(CategoryStatisticInfo.newBuilder().setArea(statisticinfo.AREA).setCount(statisticinfo.COUNT).setLength(statisticinfo.LENGTH).setCategory(entry.getKey()).build());
            }

            done.run(responseBuilder.build());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
