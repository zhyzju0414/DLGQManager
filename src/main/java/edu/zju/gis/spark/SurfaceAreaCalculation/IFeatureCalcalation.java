package edu.zju.gis.spark.SurfaceAreaCalculation;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.GridArchitectureInterface;
import edu.zju.gis.gncstatistic.Sheet;
import edu.zju.gis.gncstatistic.SheetGridArchitecture;
import edu.zju.gis.spark.SurfaceAreaCalculation.CalculationCondition.CalculationCondition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Zhy on 2017/9/21.
 */
public interface IFeatureCalcalation {
    JavaPairRDD<String, Iterable<String>> getFilteredRDD(CalculationCondition condition,List<String> sheetCode, String lcraFilePath, JavaSparkContext sc) throws Exception;

    static List<String> getSheetRangeByWkt(String geoWkt){
        //先根据geoWkt生成MBR  选出分幅
        List<String> sheetCode=new ArrayList<String>();
        GridArchitectureInterface sheetArchitecture = GridArchitectureFactory.GetGridArchitecture(50000, "sheet");
        Geometry geo = GeometryEngine.geometryFromWkt(geoWkt, 0, Geometry.Type.Unknown);
        Envelope2D boundary = new Envelope2D();
        geo.queryEnvelope2D(boundary);
//        System.out.println(boundary.xmin+" "+boundary.ymin+" "+boundary.xmax+" "+boundary.ymax );
        Iterable<Sheet> sheetRange = ((SheetGridArchitecture) sheetArchitecture).GetRangeSheets(boundary);
        for(Sheet sheet:sheetRange){
            sheetCode.add(sheet.toString());
        }
        return sheetCode;
    }
}
