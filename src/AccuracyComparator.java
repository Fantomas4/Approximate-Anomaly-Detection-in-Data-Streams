import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class AccuracyComparator {
    String baselineFilename;
    String testFilename;
    ArrayList<Integer> baselineOutliers = new ArrayList<>();
    ArrayList<Integer> testOutliers = new ArrayList<>();
    ArrayList<Integer> commonOutliers;

    public void execute() {
        baselineOutliers = loadFile(baselineFilename);
        testOutliers = loadFile(testFilename);
        findCommonOutliers();
    }

    private void findCommonOutliers() {
        commonOutliers = new ArrayList<>(baselineOutliers);
        commonOutliers.retainAll(testOutliers);
    }

    private void printStatistics() {
        int baselineSize = baselineOutliers.size();
        int testSize = testOutliers.size();
        int commonSize = commonOutliers.size();
        float percentageDetected = ((float) commonSize / baselineSize) * 100;

        System.out.println("--------------------- Baseline - Test Outlier Accuracy Comparison  ---------------------");
        System.out.println("> Baseline algorithm - Number of pure outliers: "+ baselineSize);
        System.out.println("> Test algorithm - Number of pure outliers: "+ testSize);
        System.out.printf("> Number of Baseline algorithm's outliers detected by Test algorithm: %d (%.2f %%)\n", commonSize, percentageDetected);
        System.out.println("----------------------------------------------------------------------------------------");

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
                    case "--baselineFile":
                        this.baselineFilename = args[i + 1];
                        break;
                    case "--testFile":
                        this.testFilename = args[i + 1];
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
