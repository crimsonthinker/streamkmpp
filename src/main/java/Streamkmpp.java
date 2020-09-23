import moa.cluster.Cluster;
import moa.clusterers.streamkm.StreamKM;
import moa.cluster.Clustering;
import moa.core.TimingUtils;
import moa.streams.clustering.SimpleCSVStream;
import com.yahoo.labs.samoa.instances.Instance;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import java.io.IOException;

public class Streamkmpp {

    public Streamkmpp(){
    }

    public void run(int numInstances){
        StreamKM learner = new StreamKM();
        SimpleCSVStream stream = new SimpleCSVStream();
        String dataFile = "../coresetsparkstreaming/datasets/hawk_5_0.47.txt";
        stream.csvFileOption = new FileOption("csvFile", 'f', "CSV file to load.", dataFile, "csv", false);
        stream.splitCharOption = new StringOption("splitChar", 's', "Input CSV split character", " ");

        learner.lengthOption = new IntOption("length", 'l', "Length of the data stream (n).", 1000, 0, Integer.MAX_VALUE);
        learner.sizeCoresetOption = new IntOption("sizeCoreset", 's', "Size of the coreset (m).", 200);

        stream.prepareForUse();

        learner.setModelContext(stream.getHeader());
        learner.prepareForUse();

        boolean preciseCPUTiming = TimingUtils.enablePreciseTiming();
        long evaluateStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread();
        int row = 0;
        while (stream.hasMoreInstances()) {
            Instance trainInst = stream.nextInstance().getData();

            System.out.println("Process row " + row);
            row = row + 1;
            learner.trainOnInstanceImpl(trainInst);
        }
        double time = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread()- evaluateStartTime);
        System.out.println("Processed in "+ time +" seconds.");

        Clustering result = learner.getClusteringResult();
    }

    public static void main(String[] args) throws IOException {
        Streamkmpp exp = new Streamkmpp();
        exp.run(1000000);
    }
}