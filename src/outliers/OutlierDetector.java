package outliers;


import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

public class OutlierDetector {
    protected Random random;
    protected int iMaxMemUsage = 0;
    protected int nRangeQueriesExecuted = 0;
    protected Long nTotalRunTime = 0L;
    protected double nTimePerObj;

    private TreeSet<Outlier> outliersFound;
    private Long m_timePreObjSum;
    private int nProcessed;
    private static final int m_timePreObjInterval = 100;

    private String chosenAlgorithm;
    private int windowSize;
    private int slideSize;
    private double rParameter;
    private double kParameter;
    private String dataFile;




    public void readArguments(String[] args) {
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










    protected void UpdateMaxMemUsage() {
        int x = GetMemoryUsage();
        if (iMaxMemUsage < x) iMaxMemUsage = x;
    }

    protected void Init() {
        random = new Random(System.currentTimeMillis());
        outliersFound = new TreeSet<Outlier>();

        m_timePreObjSum = 0L;
        nProcessed = 0;
        nTimePerObj = 0L;

        StdPrintMsg printer = new StdPrintMsg();
        printer.RedirectToDisplay();

        //timePerObjInfo = new MyTimePerObjInfo();
        //timePerObjInfo.Init();

        SetUserInfo(true, false, printer, 1000);
    }

    public void processNewInstanceImpl(Instance inst) {
        Long nsNow = System.nanoTime();

        ProcessNewStreamObj(inst);

        UpdateMaxMemUsage();
        nTotalRunTime += (System.nanoTime() - nsNow) / (1024 * 1024);

        // update process time per object
        nProcessed++;
        m_timePreObjSum += System.nanoTime() - nsNow;
        if (nProcessed % m_timePreObjInterval == 0) {
            nTimePerObj = ((double) m_timePreObjSum) / ((double) m_timePreObjInterval);
            if (bShowProgress) ShowTimePerObj();
            // init
            m_timePreObjSum = 0L;
        }
    }

    private void ShowTimePerObj() {
        double ms = nTimePerObj / (1000.0 * 1000.0);
        System.out.println("Process time per object (ms): " + String.format("%.3f", ms));
    }

    public Set<Outlier> GetOutliersFound() {
        return outliersFound;
    }

    protected boolean IsNodeIdInWin(long id) {
        throw new UnsupportedOperationException("Not yet implemented");
    }


    public void AddOutlier(Outlier newOutlier) {
        outliersFound.add(newOutlier);
    }

    public void RemoveOutlier(Outlier outlier) {
        outliersFound.remove(outlier);
    }

    protected int GetMemoryUsage() {
        int iMemory = (int) ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        return iMemory;
    }
}
