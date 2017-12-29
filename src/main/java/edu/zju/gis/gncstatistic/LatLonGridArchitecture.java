package edu.zju.gis.gncstatistic;

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import scala.Tuple2;

import java.io.Serializable;

/**
 * 构建经纬度格网，从左上角起算
 * Created by Frank on 2017/6/15.
 */
public class LatLonGridArchitecture implements GridArchitectureInterface<Grid>, Serializable {

    public double gridSize = 0.01;

    public LatLonGridArchitecture(double gridSize){
        this.gridSize = gridSize;
    }


    @Override
    public Tuple2<Grid, Grid> GetCellRowRange(Envelope2D envelope) {

        long startCol =  (long)(envelope.xmin/gridSize);
        long startRow = (long)((180-envelope.ymax)/gridSize);
        long endCol = (long)(envelope.xmax/gridSize);
        long endRow = (long)((180-envelope.ymin)/gridSize);
        Grid startGrid = new Grid(startRow,startCol);
        Grid endGrid = new Grid(endRow,endCol);
        return new Tuple2<Grid,Grid>(startGrid,endGrid);
    }

    @Override
    public Envelope GetSpatialRange(Grid grid) {
        double xmin = grid.Col*gridSize;
        double ymin = 180-(grid.Row+1)*gridSize;
        double xmax = (grid.Col+1)*gridSize;
        double ymax = 180-grid.Row*gridSize;
        return new Envelope(xmin,ymin,xmax,ymax);
    }

}
