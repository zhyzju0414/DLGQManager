package edu.zju.gis.gncstatistic;

/**
 * Created by Frank on 2017/6/15.
 */
public class GridArchitectureFactory {

    public static GridArchitectureInterface GetGridArchitecture(double gridSize, String gridType) {

        GridArchitectureInterface gridArchitecture = null;
        if (gridType.equals("latlon")) {
            gridArchitecture = new LatLonGridArchitecture(gridSize);
        } else if (gridType.equals("sheet")) {
            gridArchitecture = new SheetGridArchitecture((int) gridSize);
        }
        return gridArchitecture;
    }
}
