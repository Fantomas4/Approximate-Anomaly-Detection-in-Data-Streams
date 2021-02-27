package algorithms;


import java.io.*;
import java.util.*;


public class Stream {
    private final Queue<StreamObj> dataStream;


    public Stream() {
        dataStream = new ArrayDeque<StreamObj>();
    }


    public boolean hasNext() {
        return !dataStream.isEmpty();
    }

    public ArrayList<StreamObj> getIncomingData(int length) {
        ArrayList<StreamObj> results = new ArrayList<>();

        int dataSize = Math.min(dataStream.size(), length);
        for (int i = 0; i < dataSize; i++) {
            results.add(this.dataStream.remove());
        }

        return results;
    }

    public void loadFile(String filename, boolean containsClass) {
        try {
            BufferedReader bfr = new BufferedReader(new FileReader(filename));
            String line;
            try {
                while ((line = bfr.readLine()) != null) {
                    String[] atts = line.split(",");
                    int nAttributes;
                    if (containsClass) {
                        nAttributes = atts.length - 1;
                    } else {
                        nAttributes = atts.length;
                    }
                    double[] d = new double[nAttributes];
                    for (int i = 0; i < d.length; i++) {

                        d[i] = Double.parseDouble(atts[i]);
                    }
                    StreamObj streamObj = new StreamObj(d);
                    dataStream.add(streamObj);
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Stream stream = new Stream();
        stream.loadFile("G:\\IdeaProjects\\MCOD-ApproxMCOD\\datasets\\tao.txt", false);
        System.out.println("done");
    }

}


