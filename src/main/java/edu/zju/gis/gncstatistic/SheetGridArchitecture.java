package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by xwhsky on 2017/8/6.
 */
public class SheetGridArchitecture implements GridArchitectureInterface<Sheet>, Serializable {

    public int scale =10000;  //默认比例尺：1:10000比例尺

    public SheetGridArchitecture(int gridSize){
        this.scale = gridSize;
    }

    /**
     * 获取envelope跨越的分幅号范围
     * @param envelope
     * @return 结果中包含两个Sheet对象，第一个代表西北角的分幅，第二个代表东南角的分幅
     */
    @Override
    public Tuple2<Sheet, Sheet> GetCellRowRange(Envelope2D envelope) {
        int scale = this.scale;
        String upperLeftSheetCode = SheetStandardImp.CalcSheetCode(envelope.getUpperLeft().getY(), envelope.getUpperLeft().getX(), scale);
        String lowerRightSheetCode = SheetStandardImp.CalcSheetCode(envelope.getLowerRight().getY(), envelope.getLowerRight().getX(), scale);
        return new Tuple2<>(new Sheet(upperLeftSheetCode), new Sheet(lowerRightSheetCode));
    }

    @Override
    public Envelope GetSpatialRange(Sheet grid) {
        double[] westSouthPoint = SheetStandardImp.CalWestSouthPointOfSheetCode(grid.SheetCode);
        int iScale = grid.Scale;
        double deltaLat = SheetStandardImp.ScaledGridSLatLonSizeMap.get(iScale)[0];
        double deltaLon = SheetStandardImp.ScaledGridSLatLonSizeMap.get(iScale)[1];
        Envelope envelope = new Envelope(westSouthPoint[1], westSouthPoint[0], westSouthPoint[1] + deltaLon, westSouthPoint[0] + deltaLat);
        return envelope;
    }


    public Iterable<Sheet> GetRangeSheets(Envelope2D envelope) {
        ArrayList<Sheet> sheets = new ArrayList();
        Tuple2<Sheet, Sheet> tuple2 =  GetCellRowRange(envelope);
        Sheet startSheet = tuple2._1();
        int startRowIndex = tuple2._1().Row;
        int startColIndex = tuple2._1().Col;
        int endRowIndex= tuple2._2().Row;
        int endColIndex = tuple2._2().Col;

        for (int i=startRowIndex;i<=endRowIndex;i++){
            for (int j=startColIndex;j<=endColIndex;j++){
                Sheet sheet = new Sheet(i,j,startSheet.Scale,startSheet.ParentRow,startSheet.ParentCol);
                sheets.add(sheet);
            }
        }
        return  sheets;
    }



}
