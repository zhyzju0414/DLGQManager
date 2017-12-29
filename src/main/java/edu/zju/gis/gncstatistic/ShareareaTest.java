package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.WktImportFlags;
import edu.zju.gis.hbase.tool.MeasureClass;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by gsz on 2017/6/26.
 */
public class ShareareaTest {

        public static void main(String[] args) {

            File file = new File("C:\\Users\\gsz\\Desktop\\TEST.txt");
            try {
                FileReader fr = new FileReader(file);
                BufferedReader reader = new BufferedReader(fr);
                String wkt = reader.readLine();
                System.out.println(wkt);
                System.out.println(GeometryEngine.geometryFromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Polygon).calculateArea2D());
                System.out.println(MeasureClass.EllipsoidArea(wkt));
            } catch (Exception e) {
                e.printStackTrace();
            }
            }
}
