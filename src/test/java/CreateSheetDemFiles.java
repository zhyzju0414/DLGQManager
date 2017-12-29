import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import edu.zju.gis.gncstatistic.*;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Layer;
import org.gdal.ogr.ogr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateSheetDemFiles {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.loadLibrary("gdaljni");
        gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
        gdal.SetConfigOption("SHAPE_ENCODING", "GB2312");
        gdal.AllRegister();
        ogr.RegisterAll();
        String srcLcraVectorFile = "C:\\Users\\Administrator\\Desktop\\bjshp\\bj.shp";
        String srcDemTifFile = "D:\\DATA\\dem\\dem.tif";
        String gdalWarpFile = "C:\\Program Files\\Java\\jdk1.8.0_74\\jre\\bin\\gdalwarp.exe";
        String unprojectedPath = "D:\\DATA\\fenfu\\DEM2\\UnProjected";
        String projectedPath = "D:\\DATA\\fenfu\\DEM2\\Projected";
        DataSource dataSource = ogr.Open(srcLcraVectorFile, 0);
        Layer layer = dataSource.GetLayer("bj");
        double[] extent = layer.GetExtent();
        double minLon = extent[0];
        double maxLon = extent[1];
        double minLat = extent[2];
        double maxLat = extent[3];

//        minLon=115;
//        maxLon=116;
//        minLat=30;
//        maxLat=31;

        int gridSize = 50000;  //10米格网
        double deltaLat = SheetStandardImp.ScaledGridSLatLonSizeMap.get(gridSize)[0];
        double deltaLon = SheetStandardImp.ScaledGridSLatLonSizeMap.get(gridSize)[1];

        GridArchitectureInterface gridArchitectureInterface = GridArchitectureFactory.GetGridArchitecture(gridSize, "sheet");
        SheetGridArchitecture sheetGridArchitecture = (SheetGridArchitecture) gridArchitectureInterface;

        Envelope2D envelope2D = new Envelope2D(minLon, minLat, maxLon, maxLat);
        Iterable<Sheet> sheets = sheetGridArchitecture.GetRangeSheets(envelope2D);
        for (Sheet sheet:sheets){
            System.out.println(sheet.toString());
        }



        GetProj("");
        //String proj = GetProj(srcDemTifFile);
        for (Sheet sheet : sheets) {
            //生成DEM数据、先做经纬度的
            Envelope envelope2D1 = sheetGridArchitecture.GetSpatialRange(sheet);
            List<String> params = new ArrayList();
            params.add(" -t_srs " + "EPSG:4490");
            params.add(" -te " + envelope2D1.getXMin() + " " + envelope2D1.getYMin() + " " + envelope2D1.getXMax() + " " + envelope2D1.getYMax());
            //  params.add(" -tr " + deltaLon + " " + deltaLat);
            params.add(" -r bilinear");
            params.add(" " + srcDemTifFile);
            String destFile = unprojectedPath + "\\" + sheet.toString() + ".tif";
            params.add(" " + destFile);
         //   ExectOuterProcess(gdalWarpFile, params);

            //转投影
            params = new ArrayList<>();
            params.add(" -t_srs " + "EPSG:102113");
            params.add(" -tr 2 2");
            params.add(" -r bilinear");
            params.add(" " + destFile);
            String projectedFile = projectedPath + "\\" + sheet.toString() + ".tif";
            params.add(" " + projectedFile);
            ExectOuterProcess(gdalWarpFile, params);
        }

    }

    static String GetProj(String rasterFile){
        Dataset dataset =  gdal.OpenShared(rasterFile);
        String projection =dataset.GetProjectionRef();
        return  projection;
    }

    private  static void ExectOuterProcess(String exePath, List<String> parms) throws IOException, InterruptedException {
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < parms.size(); i++) {
            strBuilder.append(parms.get(i));
        }
        String newString = strBuilder.toString();

        Process process = Runtime.getRuntime().exec(exePath + " " + newString);
        process.waitFor();
    }


}
