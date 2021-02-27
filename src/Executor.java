import algorithms.ISBIndex;
import algorithms.MCOD.MCOD;
import algorithms.Stream;
import outliers.OutlierDetector;

import java.util.HashMap;

public class Executor {
    private int iMaxMemUsage = 0;
    private Long nTotalRunTime = 0L;
    private double nTimePerObj;
    private Long m_timePreObjSum;
    private int nProcessed;
    private static final int m_timePreObjInterval = 100;

    private String chosenAlgorithm;
    private int windowSize;
    private int slideSize;
    private double rParameter;
    private int kParameter;
    private String dataFile;

    private Stream stream;
    private MCOD mcodObj;


    public Executor(String[] args) {
        m_timePreObjSum = 0L;
        nProcessed = 0;
        nTimePerObj = 0L;

        readArguments(args);
        stream = new Stream();
        stream.loadFile(dataFile);
        if (chosenAlgorithm.equals("MCOD")) {
            mcodObj = new MCOD(windowSize, slideSize, rParameter, kParameter);
        }
    }

    public void performOutlierDetection() {
        while (stream.hasNext()) {
            addNewStreamObjects();
        }
    }

    private void readArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {

            //check if arg starts with --
            String arg = args[i];
            if (arg.indexOf("--") == 0) {
                switch (arg) {
                    case "--algorithm":
                        this.chosenAlgorithm = args[i + 1];
                        break;
                    case "--W":
                        this.windowSize = Integer.parseInt(args[i + 1]);
                        break;
                    case "--slide":
                        this.slideSize = Integer.parseInt(args[i + 1]);
                        break;
                    case "--R":
                        this.rParameter = Double.parseDouble(args[i + 1]);
                        break;
                    case "--k":
                        this.kParameter = Integer.parseInt(args[i + 1]);
                        break;
                    case "--datafile":
                        this.dataFile = args[i + 1];
                        break;
                }
            }
        }
    }

    public void addNewStreamObjects() {
        Long nsNow = System.nanoTime();

        mcodObj.ProcessNewStreamObjects(stream.getIncomingData(slideSize));

        UpdateMaxMemUsage();
        nTotalRunTime += (System.nanoTime() - nsNow) / (1024 * 1024);

        // update process time per object
        nProcessed++;
        m_timePreObjSum += System.nanoTime() - nsNow;
        if (nProcessed % m_timePreObjInterval == 0) {
            nTimePerObj = ((double) m_timePreObjSum) / ((double) m_timePreObjInterval);
            // init
            m_timePreObjSum = 0L;
        }
    }

    private void printResults() {
        int nRangeQueriesExecuted = mcodObj.getnRangeQueriesExecuted();
        HashMap<String, Integer> results = mcodObj.getResults();
        int nBothInlierOutlier = results.get("nBothInlierOutlier");
        int nOnlyInlier = results.get("nOnlyInlier");
        int nOnlyOutlier = results.get("nOnlyOutlier");

        System.out.println("Statistics:\n\n");
        int sum = nBothInlierOutlier + nOnlyInlier + nOnlyOutlier;
        if (sum > 0) {
            System.out.println(String.format("  Nodes always inlier: %d (%.1f%%)\n", nOnlyInlier, (100 * nOnlyInlier) / (double)sum));
            System.out.println(String.format("  Nodes always outlier: %d (%.1f%%)\n", nOnlyOutlier, (100 * nOnlyOutlier) / (double)sum));
            System.out.println(String.format("  Nodes both inlier and outlier: %d (%.1f%%)\n", nBothInlierOutlier, (100 * nBothInlierOutlier) / (double)sum));

            System.out.println("  (Sum: " + sum + ")\n");
        }

        System.out.println("\n  Total range queries: " + nRangeQueriesExecuted + "\n");
        System.out.println("  Max memory usage: " + iMaxMemUsage + " MB\n");
        System.out.println("  Total process time: " + String.format("%.2f ms", nTotalRunTime / 1000.0) + "\n");
    }

    protected void UpdateMaxMemUsage() {
        int x = GetMemoryUsage();
        if (iMaxMemUsage < x) iMaxMemUsage = x;
    }
    protected int GetMemoryUsage() {
        int iMemory = (int) ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        return iMemory;
    }

    public static void main(String[] args) {
        Executor executor = new Executor(args);
        executor.performOutlierDetection();
        executor.printResults();
    }
}
