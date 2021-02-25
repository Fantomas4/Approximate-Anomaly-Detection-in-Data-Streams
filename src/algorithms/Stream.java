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
        for (int i = 0; i < length; i++) {
            results.add(this.dataStream.remove());
        }

        return results;

    }

    public void loadFile(String filename) {

        try {
            BufferedReader bfr = new BufferedReader(new FileReader(filename));

            String line;
            try {
                while ((line = bfr.readLine()) != null) {

                    String[] atts = line.split(",");
                    double[] d = new double[atts.length];
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
        stream.loadFile("G:\\IdeaProjects\\MCOD-ApproxMCOD\\datasets\\tao.txt");
        System.out.println("done");
    }

}


