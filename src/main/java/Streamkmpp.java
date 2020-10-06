import moa.cluster.Cluster;
import moa.clusterers.streamkm.StreamKM;
import moa.cluster.Clustering;
import moa.cluster.Cluster;
import moa.core.AutoExpandVector;
import moa.core.TimingUtils;
import moa.streams.clustering.SimpleCSVStream;
import com.yahoo.labs.samoa.instances.Instance;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import java.io.IOException;

import com.opencsv.*;
import java.io.FileReader;
import java.io.File;
import java.io.FileWriter;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class Streamkmpp {

    public Streamkmpp() {
    }

    public void run(String dataFile, String dataPath, String outputPath, String delimiter) {
        StreamKM learner = new StreamKM();
        SimpleCSVStream stream = new SimpleCSVStream();
        String inputFile = dataPath + dataFile + ".txt";

        stream.csvFileOption = new FileOption("csvFile", 'f', "CSV file to load.", inputFile, "csv", false);
        stream.splitCharOption = new StringOption("splitChar", 's', "Input CSV split character", delimiter);

        learner.lengthOption = new IntOption("length", 'l', "Length of the data stream (n).", 10000, 0, Integer.MAX_VALUE);
        learner.sizeCoresetOption = new IntOption("sizeCoreset", 's', "Size of the coreset (m).", 2000);

        stream.prepareForUse();

        learner.setModelContext(stream.getHeader());
        learner.prepareForUse();

        boolean preciseCPUTiming = TimingUtils.enablePreciseTiming();
        long evaluateStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread();
        int row = 0;
        while (stream.hasMoreInstances()) {
            Instance trainInst = stream.nextInstance().getData();
            row++;
            learner.trainOnInstanceImpl(trainInst);
        }
        double time = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread() - evaluateStartTime);
        System.out.println("Processed StreamKM++ in " + time + " seconds.");

        Clustering result = learner.getClusteringResult();
        AutoExpandVector<Cluster> clusters = result.getClustering();

        long evaluatePostProcessStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread();

        List<Double> firstPoint = readDataFirstLine(inputFile, delimiter.charAt(0));
        int pointDimension = firstPoint.size();

        // check if the centroid generated by streamkm match the correct dimension of input data
        List<List<Double>> centroidList = new ArrayList<List<Double>>();
        int clusterIndex = 0;
        while (clusterIndex < clusters.size()) {
            double[] fakeCentroid = clusters.get(clusterIndex).getCenter();
            int miniIndex = 0;
            List<Double> realCentroid = new ArrayList<Double>();

            while (miniIndex < pointDimension) {
                realCentroid.add(fakeCentroid[miniIndex]);
                miniIndex++;
            }

            centroidList.add(realCentroid);
            clusterIndex++;
        }
        postProcessData(inputFile, delimiter.charAt(0), dataFile, outputPath, 10000, clusters, centroidList);

        double time2 = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread()- evaluatePostProcessStartTime);
        System.out.println("Post Processed in " + time2 + " seconds.");
    }

    protected static boolean createFile(String path) {
        try {
            File myObj = new File(path);
            if (myObj.exists())
                myObj.delete();
            return myObj.createNewFile();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        return false;
    }

    // Java code to illustrate reading a
    // CSV file line by line
    protected static List<Double> readDataFirstLine(String file, char delimiter) {
        try {
            // Create an object of file reader
            // class with CSV file as a parameter.and
            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader, delimiter, '"', 1);
            String[] nextRecord;
            List<Double> pointList = new ArrayList<Double>();

            // we are going to read data line by line
            if ((nextRecord = csvReader.readNext()) != null) {
                for (String cell : nextRecord) {
                    pointList.add(Double.parseDouble(cell));
                }
            }
            System.out.println("Read fist line CSV done.");
            return pointList;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new ArrayList<Double>();
    }

    // Java code to illustrate reading a
    // CSV file line by line
    protected static void postProcessData(String file, char delimiter, String dataFile, String outputPath, Integer bucketSize, AutoExpandVector<Cluster> clusters, List<List<Double>> centroidList) {
        try {
            // Create an object of file reader
            // class with CSV file as a parameter.
            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader, delimiter, '"', 1);
            String[] nextRecord;
            List<List<Double>> pointList = new ArrayList<List<Double>>();

            String dataOutputFile = outputPath + dataFile + "_streamkmpp.txt";
            String imageOutputFile = dataFile + "_streamkmpp.jpg";
            int row = 1;

            // we are going to read data line by line
            if (createFile(dataOutputFile)) {
                while ((nextRecord = csvReader.readNext()) != null) {
                    List<Double> point = new ArrayList<Double>();
                    for (String cell : nextRecord) {
                        point.add(Double.parseDouble(cell));
                    }
                    pointList.add(point);
                    if (row % bucketSize == 0) {
                        List<Integer> clusterIndexList = new ArrayList<Integer>();
                        List<Double> clusterDistanceList = new ArrayList<Double>();
                        for (int i = 0; i < bucketSize; i++) {
                            double minDistanceToCentroid = Double.MAX_VALUE;
                            int minClusterIndex = 0;
                            int clusterIndex = 0;

                            while (clusterIndex < clusters.size()) {
                                double distanceToCentroid = Measure.euclideanDistance(centroidList.get(clusterIndex), pointList.get(i));
                                if (distanceToCentroid < minDistanceToCentroid) {
                                    minClusterIndex = clusterIndex;
                                    minDistanceToCentroid = distanceToCentroid;
                                }
                                clusterIndex++;
                            }
                            clusterIndexList.add(minClusterIndex);
                            clusterDistanceList.add(minDistanceToCentroid);
                        }
                        writePointToFile(pointList, clusterIndexList, clusterDistanceList, dataOutputFile);
                        pointList = new ArrayList<List<Double>>();
                        System.out.println("Process line " + row);
                    }
                    row++;
                }
            }
            System.out.println("Finish post process.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void writeFile(List<List<Double>> pointList, List<Integer> pointClusterIndexList, List<Double> distanceToCentroid, String path) {
        if (createFile(path)) {
            try {
                FileWriter myWriter = new FileWriter(path);
                int pointListSize = pointList.size();
                int index = 0;

                while (index < pointListSize) {
                    myWriter.write(pointList.get(index).toString() + "\t" + pointClusterIndexList.get(index).toString() + "\t" + distanceToCentroid.get(index).toString() + "\n" );
                    index = index + 1;
                }
                myWriter.close();
                System.out.println("Successfully wrote to the file.");
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
    }

    protected static void writePointToFile(List<List<Double>> pointList, List<Integer> pointClusterIndexList, List<Double> distanceToCentroid, String path) {
        try {
            FileWriter myWriter = new FileWriter(path, true);
            int pointListSize = pointList.size();
            int index = 0;

            while (index < pointListSize) {
                myWriter.write(pointList.get(index).toString() + "\t" + pointClusterIndexList.get(index).toString() + "\t" + distanceToCentroid.get(index).toString() + "\n" );
                index = index + 1;
            }
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    protected void saveScatterChart(String path, String plotName, List<List<Double>> pointList, List<Integer> pointClusterIndexList) {
        if (createFile(path + plotName)) {
            try {
                if (pointList.get(0).size() >= 3)
                    return;
                List<XYSeries> clusterSeries = new ArrayList<XYSeries>();
                Set<Integer> clusterIndexDistinct = new HashSet<Integer>(pointClusterIndexList);
                for (int t = 0; t < clusterIndexDistinct.size(); t++)
                    clusterSeries.add(t, new XYSeries("Cluster " + t));

                XYSeriesCollection plotData =  It takes 10 minutes for processing 20k rows (including read, calculate the nearest centroid, label and write)new XYSeriesCollection();
                int s = 0;
                for (int index = 0; index < pointClusterIndexList.size(); index++) {
                    int currentIndex = pointClusterIndexList.get(index);
                    if (currentIndex != -2 && currentIndex != -1)
                        clusterSeries.get(currentIndex).add(pointList.get(index).get(0), pointList.get(index).get(1));
                }
                for (XYSeries x : clusterSeries) {
                    plotData.addSeries(x);
                }

                JFreeChart scatterChart = ChartFactory.createScatterPlot(plotName, "X-Axis", "Y-Axis", plotData);
                ChartUtilities.saveChartAsJPEG(new File(path + plotName), scatterChart, 1024, 729);
                System.out.println("Successfully saved scatter chart");
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Streamkmpp exp = new Streamkmpp();

        String fileName = "USCensus1990.data";
        String dataPath = "../coresetsparkstreaming/datasets/";
        String outputPath = "../coresetsparkstreaming/result/process/streamkmpp/";
        exp.run(fileName, dataPath, outputPath, ",");
    }
}