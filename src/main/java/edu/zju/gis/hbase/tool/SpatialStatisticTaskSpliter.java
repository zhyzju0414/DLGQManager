package edu.zju.gis.hbase.tool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;



import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest;
import edu.zju.gis.hbase.coprocessor.SpatialStatisticProtos.SpatialStatisticRequest.GetInfo;

public class SpatialStatisticTaskSpliter implements IRequestTaskSpliter<SpatialStatisticRequest> {

    SpatialStatisticRequest request;
    int taskNum;


    public SpatialStatisticTaskSpliter(SpatialStatisticRequest request) {
        super();
        this.request = request;
        this.taskNum = 3;
    }

    public SpatialStatisticTaskSpliter(SpatialStatisticRequest request,int taskNum) {
        super();
        this.request = request;
        this.taskNum = taskNum;
    }


    @Override
    public List<SpatialStatisticRequest> GetSplitTasks() {
        // TODO Auto-generated method stub
        List<GetInfo> getInfoList = request.getGetListList();

        List<SpatialStatisticRequest> arr = new ArrayList<SpatialStatisticRequest>();
        if( getInfoList.size()<taskNum){
            arr.add(request);
        }else{
            List<List<GetInfo>> getInfoarr = new ArrayList<List<GetInfo>>(taskNum);
            for(int i=0;i<taskNum;i++){
                getInfoarr.add(new ArrayList<GetInfo>());
            }

            int offset = getInfoList.size()/taskNum+1;
            int j=0;
            for(int i=0;i<getInfoList.size();i++){
                if(getInfoList.get(i).getCliprequired()){
                    j++;
                }
                getInfoarr.get(i/offset).add(getInfoList.get(i));

            }
            System.out.println(j);
            for(int i=0;i<taskNum;i++){
                arr.add(SpatialStatisticRequest.newBuilder().setGeowkt(request.getGeowkt()).addAllGetList(getInfoarr.get(i)).build());
            }
        }
        return arr;
    }

}
