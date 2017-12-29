package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;

/*
 * 基于区域的任务分片
 */
public class RegionBasedSpatialStatisticTaskSpliter implements
        IRequestTaskSpliter<SpatialStatisticRequest> {

    SpatialStatisticRequest request;



    public RegionBasedSpatialStatisticTaskSpliter(
            SpatialStatisticRequest request) {
        super();
        this.request = request;
    }



    @Override
    public List<SpatialStatisticRequest> GetSplitTasks() {
        // TODO Auto-generated method stub
//		Map<String,List<GetInfo>> map = new HashMap<String,List<GetInfo>>();
//		System.out.println(request.getGetListCount());
//		for(int i=0;i<request.getGetListCount();i++){
//			String regionCode = request.getGetList(i).getRowkey().substring(0,12);
//			if(!map.containsKey(regionCode)){
//				map.put(regionCode, new ArrayList<GetInfo>());
//			}
//			map.get(regionCode).add(request.getGetList(i));
//		}

        List<SpatialStatisticRequest> list = new ArrayList<SpatialStatisticRequest>();
        //	System.out.println(map.size());
        list.add(request);
//		for(Entry<String,List<GetInfo>> entry:map.entrySet()){
//			//如果单个任务的执行数过大，则按照平均任务划分法进行进一步划分
//
//				if(entry.getValue().size()>10000){
//					AverageNumBasedRequestTaskSpliter averageTaskSpliter = new AverageNumBasedRequestTaskSpliter(SpatialStatisticRequest.newBuilder().setGeowkt(request.getGeowkt()).addAllGetList(entry.getValue()).build());
//					list.addAll(averageTaskSpliter.GetSplitTasks());
//				}else{
//					list.add(SpatialStatisticRequest.newBuilder().setGeowkt(request.getGeowkt()).addAllGetList(entry.getValue()).build());
//				}
//
//
//		}
//		System.out.println(map.size());
//		System.out.println(list.size());
        return list;
    }

}
