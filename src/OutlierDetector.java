import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

public class OutlierDetector {
    public static class Outlier implements Comparable<Outlier> {
        public long id;
        public Instance inst;
        public Object obj;

        public Outlier(Instance inst, long id, Object obj) {
            this.id = id;
            this.inst = inst;
            this.obj = obj;
        }

        @Override
        public int compareTo(Outlier o) {
            if (this.id > o.id)
                return 1;
            else if (this.id < o.id)
                return -1;
            else
                return 0;
        }

        @Override
        public boolean equals(Object o) {
            return (this.id == ((Outlier) o).id);
        }
    }

    public static abstract class OutlierNotifier {
        public void OnOutlier(Outlier outlier) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public void OnInlier(Outlier outlier) {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }


    public OutlierNotifier outlierNotifier = null;

    protected Random random;
    protected int iMaxMemUsage = 0;
    protected int nRangeQueriesExecuted = 0;
    protected Long nTotalRunTime = 0L;
    protected double nTimePerObj;

    private Clustering myClusters = null;
    private TreeSet<Outlier> outliersFound;
    private Long m_timePreObjSum;
    private int nProcessed;
    private static final int m_timePreObjInterval = 100;

    protected void UpdateMaxMemUsage() {
        int x = GetMemoryUsage();
        if (iMaxMemUsage < x) iMaxMemUsage = x;
    }

    public double getTimePerObj() {
        return nTimePerObj;
    }

    public String getObjectInfo(Object obj) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public String getStatistics() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public double[] getInstanceValues(Instance inst) {
        int length = inst.numValues()-1;
        double[] values = new double[length]; // last attribute is the class
        for (int i = 0; i < length; i++) {
            values[i] = inst.value(i);
        }
        return values;
    }

    public void PrintInstance(Instance inst) {
        Print("instance: [ ");
        for (int i = 0; i < inst.numValues()-1; i++) { // -1 last value is the class
            Printf("%.2f ", inst.value(i));
        }
        Print("] ");
        Println("");
    }

    @Override
    public void resetLearningImpl() {
        Init();
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

    @Override
    public void trainOnInstanceImpl(Instance inst) {
        processNewInstanceImpl(inst);
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
        Println("Process time per object (ms): " + String.format("%.3f", ms));
    }

    protected void ProcessNewStreamObj(Instance inst) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    public void PrintOutliers() {
        Print("Outliers: ");
        for (Outlier o : outliersFound) {
            Printf("[%d] ", o.id);
        }
        Println("");
    }

    public Set<Outlier> GetOutliersFound() {
        return outliersFound;
    }

    protected boolean IsNodeIdInWin(long id) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Clustering getClusteringResult(){
        myClusters = new Clustering();
        for (Outlier o : outliersFound) {
            if (IsNodeIdInWin(o.id)) {
                double[] center = new double[o.inst.numValues() - 1];
                for (int i = 0; i < o.inst.numValues() - 1; i++) {
                    center[i] = o.inst.value(i);
                }
                Cluster c = new SphereCluster(center, 0);
                myClusters.add(c);
            }
        }
        return myClusters;
    }

    public Vector<Outlier> getOutliersResult(){
        Vector<Outlier> outliers = new Vector<Outlier>();
        for (Outlier o : outliersFound) {
            if (IsNodeIdInWin(o.id)) {
                outliers.add(o);
            }
        }
        return outliers;
    }

    protected void AddOutlier(Outlier newOutlier) {
        boolean bNewAdd = outliersFound.add(newOutlier);
        if ((outlierNotifier != null) && bNewAdd) {
            outlierNotifier.OnOutlier(newOutlier);
        }
    }

    protected boolean RemoveExpiredOutlier(Outlier outlier) {
        boolean bFound = outliersFound.remove(outlier);
        return bFound;
    }

    protected boolean RemoveOutlier(Outlier outlier) {
        boolean bFound = outliersFound.remove(outlier);
        if ((outlierNotifier != null) && bFound) {
            outlierNotifier.OnInlier(outlier);
        }
        return bFound;
    }

    @Override
    public boolean implementsMicroClusterer() {
        return false;
    }

    @Override
    public Clustering getMicroClusteringResult() {
        return null;
    }

    @Override
    protected Measurement[] getModelMeasurementsImpl() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void getModelDescription(StringBuilder out, int indent) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isRandomizable() {
        return false;
    }

    @Override
    public boolean keepClassLabel(){
        return true;
    }

    @Override
    public double[] getVotesForInstance(Instance inst) {
        return null;
    }

    // show progress through object
    protected ProgressInfo myProgressInfo;
    protected PrintMsg myOut;
    protected boolean bTrace = false;
    protected boolean bShowProgress = false;
    public boolean bStopAlgorithm = false;
    // time measurement for progress
    private Long _msPrev = 0L, _msNow = 0L;

    public void SetShowProgress(boolean b) {
        bShowProgress = b;
    }

    public void SetTrace(boolean b) {
        bTrace = b;
    }

    public void SetProgressInterval(int iProgressInterval) {
        myProgressInfo = new MyProgressInfo(iProgressInterval);
    }

    public void SetMessagePrinter(PrintMsg logPrinter) {
        myOut = logPrinter;
    }

    public void SetUserInfo(
            boolean bShowProgress,
            boolean bTrace,
            PrintMsg logPrinter,
            int iProgressInterval)
    {
        this.bShowProgress = bShowProgress;
        this.bTrace = bTrace;

        myProgressInfo = new MyProgressInfo(iProgressInterval);
        myOut = logPrinter;
    }

    public interface PrintMsg {
        public void print(String s);
        public void println(String s);
        public void printf(String fmt, Object... args);
    }

    public interface ProgressInfo {
        public int GetInterval();
        public void ShowProgress(String sMsg);
    }

    public class StdPrintMsg implements PrintMsg {
        private PrintStream printStream, fileStream;

        public StdPrintMsg() {
            RedirectToDisplay();
        }

        public StdPrintMsg(String sFilename) {
            RedirectToFile(sFilename);
        }

        public void RedirectToDisplay() {
            printStream = System.out;
        }

        public void RedirectToFile(String sFilename) {
            File file = new File(sFilename);
            try {
                fileStream = new PrintStream(new FileOutputStream(file));
                printStream = fileStream;
            } catch (Exception ex) {}
        }

        public void RedirectToFile() {
            printStream = fileStream;
        }

        @Override
        public void println(String s) {
            printStream.println(s);
        }

        @Override
        public void print(String s) {
            printStream.print(s);
        }

        @Override
        public void printf(String fmt, Object... args) {
            printStream.printf(fmt, args);
        }
    }

    class MyProgressInfo implements ProgressInfo {
        int progressInterval;

        public MyProgressInfo(int interval) {
            progressInterval = interval;
        }

        public void setProgressInterval(int progressInterval) {
            this.progressInterval = progressInterval;
        }

        @Override
        public int GetInterval() {
            return progressInterval;
        }

        @Override
        public void ShowProgress(String sMsg) {
            myOut.println(sMsg);
        }
    }

    protected void ShowProgress(String sMsg)
    {
        ShowProgress(sMsg, false);
    }

    protected void ShowProgress(String sMsg, boolean bShowAlways) {
        if (bShowAlways || (_msNow - _msPrev >= myProgressInfo.GetInterval())) {
            // call user progress function
            myProgressInfo.ShowProgress(sMsg);

            _msNow = System.currentTimeMillis();
            _msPrev = _msNow;
        } else {
            _msNow = System.currentTimeMillis();
        }
    }

    protected void Println(String s) {
        if (myOut != null)
            myOut.println(s);
    }

    protected void Print(String s) {
        if (myOut != null)
            myOut.print(s);
    }

    protected void Printf(String fmt, Object... args) {
        if (myOut != null)
            myOut.printf(fmt, args);
    }

    protected int GetMemoryUsage() {
        int iMemory = (int) ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        return iMemory;
    }
}
