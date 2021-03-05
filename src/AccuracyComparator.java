import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class AccuracyComparator {
    String mcodFilename;
    String approxmcodFilename;
    ArrayList<Integer> mcodOutliers = new ArrayList<>();
    ArrayList<Integer> approxmcodOutliers = new ArrayList<>();
    ArrayList<Integer> commonOutliers;

    public void execute() {
        mcodOutliers = loadFile(mcodFilename);
        approxmcodOutliers = loadFile(approxmcodFilename);
        findCommonOutliers();
    }

    private void findCommonOutliers() {
        commonOutliers = new ArrayList<>(mcodOutliers);
        commonOutliers.retainAll(approxmcodOutliers);
    }

    private void printStatistics() {
        int mcodSize = mcodOutliers.size();
        int approxmcodSize = approxmcodOutliers.size();
        int commonSize = commonOutliers.size();
        float percentageDetected = ((float) commonSize / mcodSize) * 100;

        System.out.println("-------------------- MCOD - ApproxMCOD Outlier Accuracy Comparison  --------------------");
        System.out.println("> MCOD - Number of pure outliers: "+ mcodSize);
        System.out.println("> ApproxMCOD - Number of pure outliers: "+ approxmcodSize);
        System.out.printf("> Number of MCOD's outliers detected by ApproxMCOD: %d (%.2f %%)\n", commonSize, percentageDetected);
        System.out.println("--------------------------------------------------------------------------------");

    }

    private ArrayList<Integer> loadFile(String filename) {
        ArrayList<Integer> loadedData = new ArrayList<>();

        try {
            BufferedReader bfr = new BufferedReader(new FileReader(filename));
            String line;
            try {
                while ((line = bfr.readLine()) != null) {
                    loadedData.add(Integer.parseInt(line));
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return loadedData;
    }

    public void readArguments(String[] args) {
        for (int i = 0; i < args.length; i++) {

            //check if arg starts with --
            String arg = args[i];
            if (arg.indexOf("--") == 0) {
                switch (arg) {
                    case "--mcodFile":
                        this.mcodFilename = args[i + 1];
                        break;
                    case "--approxmcodFile":
                        this.approxmcodFilename = args[i + 1];
                        break;
                }
            }
        }
    }

    public static void main(String[] args) {
        AccuracyComparator accComparator = new AccuracyComparator();
        accComparator.readArguments(args);
        accComparator.execute();
        accComparator.printStatistics();
    }
}
