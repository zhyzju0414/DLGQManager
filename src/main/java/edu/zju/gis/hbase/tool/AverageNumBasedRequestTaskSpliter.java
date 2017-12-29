package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.List;

import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;

/*
 * 基于平均数量分配的任务分配策略，如果一个任务中包含5000个计算，若平均只允许1000个作为一个计算任务，则任务数为10000/10
 * ，若线程数超过了maxTaskNum，则计算任务数为maxTaskNum
 */
public class AverageNumBasedRequestTaskSpliter implements IRequestTaskSpliter<SpatialStatisticRequest> {

    private int averageNum;
    private SpatialStatisticRequest request;
    private int maxTaskNum;

    public AverageNumBasedRequestTaskSpliter(int averageNum,
                                             SpatialStatisticRequest request, int maxTaskNum) {
        super();
        this.averageNum = averageNum;
        this.request = request;
        this.maxTaskNum = maxTaskNum;
    }

    public AverageNumBasedRequestTaskSpliter(SpatialStatisticRequest request) {
        super();
        this.averageNum = 10000;
        this.request = request;
        this.maxTaskNum =  2;
    }


    @Override
    public List<SpatialStatisticRequest> GetSplitTasks() {
        // TODO Auto-generated method stub
        int targettaskNum = request.getGetListCount()/averageNum>maxTaskNum?maxTaskNum:request.getGetListCount()/averageNum;
        averageNum = request.getGetListCount()/targettaskNum;

        List<GetInfo> getInfoList = request.getGetListList();

        List<SpatialStatisticRequest> arr = new ArrayList<SpatialStatisticRequest>();

        if(targettaskNum<1){
            arr.add(request);
            return arr;
        }

        List<List<GetInfo>> getInfoarr = new ArrayList<List<GetInfo>>(targettaskNum);
        for(int i=0;i<targettaskNum;i++){
            getInfoarr.add(new ArrayList<GetInfo>());
        }

        int offset = getInfoList.size()/targettaskNum+1;
        for(int i=0;i<getInfoList.size();i++){
            getInfoarr.get(i/offset).add(getInfoList.get(i));
        }
        for(int i=0;i<targettaskNum;i++){
            arr.add(SpatialStatisticRequest.newBuilder().setGeowkt(request.getGeowkt()).addAllGetList(getInfoarr.get(i)).build());
        }
        return arr;
    }

}
