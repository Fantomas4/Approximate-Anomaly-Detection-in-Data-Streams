import algorithms.ApproxMCOD;
import algorithms.MCOD;
import core.Outlier;
import core.Stream;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Set;

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
    private boolean containsClass;
    private String outliersFile;

    // ApproxMCOD additional parameters
    private int pdLimit;
    private double arFactor;

    private Stream stream;

    private MCOD mcodObj;
    private ApproxMCOD approxMCODObj;


    public Executor(String[] args) {
        m_timePreObjSum = 0L;
        nProcessed = 0;
        nTimePerObj = 0L;

        readArguments(args);
        stream = new Stream();
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
                    case "--pdLimit":
                        this.pdLimit = Integer.parseInt(args[i + 1]);
                        break;
                    case "--arFactor":
                        this.arFactor = Double.parseDouble(args[i + 1]);
                        break;
                    case "--datafile":
                        this.dataFile = args[i + 1];
                        break;
                    case "--containsClass":
                        this.containsClass = Boolean.parseBoolean(args[i + 1]);
                        break;
                    case "--outliersFile":
                        this.outliersFile = args[i + 1];
                        break;
                }
            }
        }
    }

    public void performOutlierDetection() {
        // Load dataset file
        stream.loadFile(dataFile, containsClass);

        if (chosenAlgorithm.equals("MCOD")) {
            mcodObj = new MCOD(windowSize, slideSize, rParameter, kParameter);
        } else if (chosenAlgorithm.equals("ApproxMCOD")) {
            approxMCODObj = new ApproxMCOD(windowSize, slideSize, rParameter, kParameter, pdLimit, arFactor);
        }

        while (stream.hasNext()) {
            addNewStreamObjects();
        }

        // Evaluate the non-expired nodes still in the window in order to record
        // the nodes that are pure outliers.
        if (chosenAlgorithm.equals("MCOD")) {
            mcodObj.evaluateRemainingNodesInWin();

        } else if (chosenAlgorithm.equals("ApproxMCOD")) {
            approxMCODObj.evaluateRemainingNodesInWin();
        }

        Set<Outlier> outliersDetected;
        if (chosenAlgorithm.equals("MCOD")) {
            outliersDetected = mcodObj.GetOutliersFound();
            exportOutliersToFile(outliersDetected, outliersFile);
        } else if (chosenAlgorithm.equals("ApproxMCOD")) {
            outliersDetected = approxMCODObj.GetOutliersFound();
            exportOutliersToFile(outliersDetected, outliersFile);
        }
    }

    public void addNewStreamObjects() {
        Long nsNow;

        if (chosenAlgorithm.equals("MCOD")) {
            nsNow = System.nanoTime();

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
        } else if (chosenAlgorithm.equals("ApproxMCOD")) {
            nsNow = System.nanoTime();

            approxMCODObj.ProcessNewStreamObjects(stream.getIncomingData(slideSize));

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
    }

    private void exportOutliersToFile(Set<Outlier> outliersDetected, String targetFile) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(targetFile));

            for (Outlier outlier : outliersDetected) {
                bw.write(Long.toString(outlier.id));
                bw.newLine();
            }

            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, Integer> getResults() {
        HashMap<String, Integer> results = null;
        if (chosenAlgorithm.equals("MCOD")) {
            results = mcodObj.getResults();

        } else if (chosenAlgorithm.equals("ApproxMCOD")) {
            results = approxMCODObj.getResults();
        }

        return results;
    }

    public void printResults() {
        HashMap<String, Integer> results = getResults();

        int nBothInlierOutlier = results.get("nBothInlierOutlier");
        int nOnlyInlier = results.get("nOnlyInlier");
        int nOnlyOutlier = results.get("nOnlyOutlier");
        int nRangeQueriesExecuted = results.get("nRangeQueriesExecuted");

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

    private void UpdateMaxMemUsage() {
        int x = GetMemoryUsage();
        if (iMaxMemUsage < x) iMaxMemUsage = x;
    }
    private int GetMemoryUsage() {
        int iMemory = (int) ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        return iMemory;
    }

    public static void main(String[] args) {
        Executor executor = new Executor(args);
        executor.performOutlierDetection();
        executor.printResults();
    }
}
