package edu.zju.gis.spark.TransformationTool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by zhaoxianwei on 2017/5/5.
 */
public class OperationsParams   extends Configuration {

    private static final Log LOG = LogFactory.getLog(OperationsParams.class);

    /** Separator between shape type and value */
    public static final String ShapeValueSeparator = "//";

    /** Maximum number of splits to handle by a local algorithm */
    private static final int MaxSplitsForLocalProcessing = Runtime.getRuntime()
            .availableProcessors();

    private static final long MaxSizeForLocalProcessing = 200 * 1024 * 1024;

    /** All detected input paths */
    private Path[] allPaths;

    static {
        // Load configuration from files
        Configuration.addDefaultResource("spatial-default.xml");
        Configuration.addDefaultResource("spatial-site.xml");
    }

    public OperationsParams() {
        this(new Configuration());
    }




    /**
     * Initialize the command line arguments from an existing configuration and
     * a list of additional arguments.
     *
     * @param conf A set of configuration parameters to initialize to
     * @param additionalArgs Any additional command line arguments given by the user.
     */
    public OperationsParams(Configuration conf, String... additionalArgs) {
        super(conf);
        initialize(additionalArgs);
    }


    public OperationsParams(GenericOptionsParser parser) {
        super(parser.getConfiguration());
        initialize(parser.getRemainingArgs());

    }


    public void initialize(String... args) {
        // TODO if the argument shape is set to a class in a third party jar
        // file add that jar file to the archives
        Vector<Path> paths = new Vector<Path>();
        for (String arg : args) {
            String argl = arg.toLowerCase();
            if (arg.startsWith("-no-")) {
                this.setBoolean(argl.substring(4), false);
            } else if (argl.startsWith("-")) {
                this.setBoolean(argl.substring(1), true);
            } else if (argl.contains(":") ) {
                String[] parts = arg.split(":", 2);
                String key = parts[0].toLowerCase();
                String value = parts[1];
                String previousValue = this.get(key);
                if (previousValue == null)
                    this.set(key, value);
                else
                    this.set(key, previousValue + "\n" + value);
            } else {
                paths.add(new Path(arg));
            }
        }
        this.allPaths = paths.toArray(new Path[paths.size()]);
    }

    public Path[] getPaths() {
        return allPaths;
    }

    public Path getPath() {
        return allPaths.length > 0 ? allPaths[0] : null;
    }

    public Path getOutputPath() {
        return allPaths.length > 1 ? allPaths[allPaths.length - 1] : null;
    }

    public void setOutputPath(String newPath) {
        if (allPaths.length > 1) {
            allPaths[allPaths.length - 1] = new Path(newPath);
        }
    }

    public Path getInputPath() {
        return getPath();
    }

    public Path[] getInputPaths() {
        if (allPaths.length < 2) {
            return allPaths;
        }
        Path[] inputPaths = new Path[allPaths.length - 1];
        System.arraycopy(allPaths, 0, inputPaths, 0, inputPaths.length);
        return inputPaths;
    }

	/*
	 * public CellInfo[] getCells() { String cell_of = (String) get("cells-of");
	 * if (cell_of == null) return null; Path path = new Path(cell_of);
	 * FileSystem fs; try { fs = path.getFileSystem(new Configuration()); return
	 * SpatialSite.cellsOf(fs, path); } catch (IOException e) {
	 * e.printStackTrace(); } return null; }
	 */

    /**
     * Checks that there is at least one input path and that all input paths
     * exist. It treats all user-provided paths as input. This is useful for
     * input-only operations which do not take any output path (e.g., MBR and
     * Aggregate).
     *
     * @return <code>true</code> if there is at least one input path and all input
     *         paths exist.
     * @throws IOException
     */
    public boolean checkInput() throws IOException {
        Path[] inputPaths = getPaths();
        if (inputPaths.length == 0) {
            LOG.error("No input files");
            return false;
        }

        for (Path path : inputPaths) {
            // Skip existence checks for wild card input
            if (isWildcard(path))
                continue;
            FileSystem fs = path.getFileSystem(this);
            if (!fs.exists(path)) {
                LOG.error("Input file '" + path + "' does not exist");
                return false;
            }
        }

        return true;
    }

    public boolean checkInputOutput() throws IOException {
        return checkInputOutput(false);
    }

    /**
     * Makes standard checks for input and output files. It is assumes that all
     * files are input files while the last one is the output file. First, it
     * checks that there is at least one input file. Then, it checks that every
     * input file exists. After that, it checks for output file, if it exists
     * and the overwrite flag is not present, it fails.
     *
     * @return <code>true</code> if all checks pass. <code>false</code>
     *         otherwise.
     * @throws IOException If the method could not get the information of the input
     */
    public boolean checkInputOutput(boolean outputRequired) throws IOException {
        Path[] inputPaths = getInputPaths();
        if (inputPaths.length == 0) {
            LOG.error("No input files");
            return false;
        }
        for (Path path : inputPaths) {
            if (isWildcard(path))
                continue;
            FileSystem fs = path.getFileSystem(this);
            if (!fs.exists(path)) {
                LOG.error("Input file '" + path + "' does not exist");
                return false;
            }
        }
        Path outputPath = getOutputPath();
        if (outputPath == null && outputRequired) {
            LOG.error("Output path is missing");
            return false;
        }
        if (outputPath != null) {
            FileSystem fs = outputPath.getFileSystem(this);
            if (fs.exists(outputPath)) {
                if (this.getBoolean("overwrite", false)) {
                    fs.delete(outputPath, true);
                } else {
                    LOG.error("Output file '" + outputPath
                            + "' exists and overwrite flag is not set");
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkOutput() throws IOException {
        Path[] inputPaths = getInputPaths();
        Path outputPath = inputPaths[inputPaths.length - 1];
        if (outputPath != null) {
            FileSystem fs = outputPath.getFileSystem(this);
            if (fs.exists(outputPath)) {
                if (this.getBoolean("overwrite", false)) {
                    fs.delete(outputPath, true);
                } else {
                    LOG.error("Output file '" + outputPath
                            + "' exists and overwrite flag is not set");
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isWildcard(Path path) {
        return path.toString().indexOf('*') != -1
                || path.toString().indexOf('?') != -1;
    }





    private String[] getArray(String key) {
        String val = get(key);
        return val == null ? null : val.split("\n");
    }

    public long getSize(String key) {
        return getSize(this, key);
    }


    public static long getSize(Configuration conf, String key) {
        String size_str = conf.get(key);
        if (size_str == null)
            return 0;
        if (size_str.indexOf('.') == -1)
            return Long.parseLong(size_str);
        String[] size_parts = size_str.split("\\.", 2);
        long size = Long.parseLong(size_parts[0]);
        size_parts[1] = size_parts[1].toLowerCase();
        if (size_parts[1].startsWith("k"))
            size *= 1024;
        else if (size_parts[1].startsWith("m"))
            size *= 1024 * 1024;
        else if (size_parts[1].startsWith("g"))
            size *= 1024 * 1024 * 1024;
        else if (size_parts[1].startsWith("t"))
            size *= 1024 * 1024 * 1024 * 1024;
        return size;
    }

    public static int getJoiningThresholdPerOnce(Configuration conf,
                                                 String key) {
        String joiningThresholdPerOnce_str = conf.get(key);
        if (joiningThresholdPerOnce_str == null)
            LOG.error("Your joiningThresholdPerOnce is not set");
        return Integer.parseInt(joiningThresholdPerOnce_str);
    }

    public static void setJoiningThresholdPerOnce(Configuration conf,
                                                  String param, int joiningThresholdPerOnce) {
        String str;
        if (joiningThresholdPerOnce < 0){
            str = "50000";
        }else{
            str = joiningThresholdPerOnce + "";
        }
        conf.set(param, str);
    }

    public static void setFilterOnlyModeFlag(Configuration conf,
                                             String param, boolean filterOnlyMode) {
        String str;
        if (filterOnlyMode){
            str = "true";
        }else{
            str = "false";
        }
        conf.set(param, str);
    }

    public static boolean getFilterOnlyModeFlag(Configuration conf,
                                                String key) {
        String filterOnlyModeFlag = conf.get(key);
        if (filterOnlyModeFlag == null)
            LOG.error("Your filterOnlyMode is not set");
        return Boolean.parseBoolean(filterOnlyModeFlag);
    }

    public static boolean getInactiveModeFlag(Configuration conf,
                                              String key) {
        String inactiveModeFlag_str = conf.get(key);
        if (inactiveModeFlag_str == null)
            LOG.error("Your inactiveModeFlag is not set");
        return Boolean.parseBoolean(inactiveModeFlag_str);
    }

    public static void setInactiveModeFlag(Configuration conf,
                                           String param, boolean inactiveModeFlag) {
        String str;
        if (inactiveModeFlag){
            str = "true";
        }else{
            str = "false";
        }
        conf.set(param, str);
    }

    public static Path getRepartitionJoinIndexPath(Configuration conf,
                                                   String key) {
        String repartitionJoinIndexPath_str = conf.get(key);
        if (repartitionJoinIndexPath_str == null)
            LOG.error("Your index file is not set");
        return new Path(repartitionJoinIndexPath_str);
    }

    public static void setRepartitionJoinIndexPath(Configuration conf,
                                                   String param, Path indexPath) {
        String str = indexPath.toString();
        conf.set(param, str);
    }

    /** Data type for the direction of skyline to compute */
    public enum Direction {
        MaxMax, MaxMin, MinMax, MinMin
    };

    public Direction getDirection(String key, Direction defaultDirection) {
        return getDirection(this, key, defaultDirection);
    }

    public static Direction getDirection(Configuration conf, String key,
                                         Direction defaultDirection) {
        String strdir = conf.get("dir");
        if (strdir == null)
            return defaultDirection;
        Direction dir;
        if (strdir.equalsIgnoreCase("maxmax")
                || strdir.equalsIgnoreCase("max-max")) {
            dir = Direction.MaxMax;
        } else if (strdir.equalsIgnoreCase("maxmin")
                || strdir.equalsIgnoreCase("max-min")) {
            dir = Direction.MaxMin;
        } else if (strdir.equalsIgnoreCase("minmax")
                || strdir.equalsIgnoreCase("min-max")) {
            dir = Direction.MinMax;
        } else if (strdir.equalsIgnoreCase("minmin")
                || strdir.equalsIgnoreCase("min-min")) {
            dir = Direction.MinMin;
        } else {
            System.err.println("Invalid direction: " + strdir);
            System.err
                    .println("Valid directions are: max-max, max-min, min-max, and min-min");
            return null;
        }
        return dir;
    }


    public void clearAllPaths() {
        this.allPaths = null;
    }


}
