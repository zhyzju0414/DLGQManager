import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.GeometryEngine;
import edu.zju.gis.gncstatistic.GridArchitectureFactory;
import edu.zju.gis.gncstatistic.GridArchitectureInterface;
import edu.zju.gis.gncstatistic.Sheet;
import edu.zju.gis.gncstatistic.SheetGridArchitecture;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Geometry;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;

/**
 * Created by Administrator on 2017/9/6.
 */
public class sheettest {
    public static void main(String[] args) {
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
        gdal.AllRegister();
        ogr.RegisterAll();
        String srcLcraVectorFile = "C:\\Users\\Administrator\\Desktop\\bjshp\\bj.shp";
        DataSource dataSource = ogr.Open(srcLcraVectorFile, 0);
        Layer layer = dataSource.GetLayer("bj");
        Geometry geo = layer.GetFeature(0).GetGeometryRef();
        com.esri.core.geometry.Geometry geo2 = GeometryEngine.geometryFromWkt(geo.ExportToWkt(), 0, com.esri.core.geometry.Geometry.Type.Unknown);
        Envelope2D boundary = new Envelope2D();
        geo2.queryEnvelope2D(boundary);
//        Tuple2<Sheet,Sheet> sheetRange = sheetArchitecture.GetCellRowRange(boundary);
        GridArchitectureInterface sheetArchitecture = GridArchitectureFactory.GetGridArchitecture(50000, "sheet");
        Iterable<Sheet> sheetRange = ((SheetGridArchitecture) sheetArchitecture).GetRangeSheets(boundary);
        for(Sheet sheet:sheetRange){
            System.out.println(sheet.toString());
        }
    }
}
