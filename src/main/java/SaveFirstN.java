import java.io.IOException;
import java.util.ArrayList;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SaveFirstN {

    private static ArrayList<java.lang.String> findNBest(int N, org.apache.hadoop.conf.Configuration conf) throws IOException {
        ArrayList<java.lang.String> documents = new ArrayList<>();

        FileSystem f = FileSystem.get(conf);
        FSDataInputStream finalOutput = f.open(new Path(Paths.SAVEN_IN));
        BufferedReader bRead = new BufferedReader(new InputStreamReader(finalOutput));

        java.lang.String line = bRead.readLine();
        int step = 0;

        while (step < N && line != null ) {
            step +=1;
            documents.add(line);
            line = BufRead.readLine();
        }
        return result;
    }

    public static int run(java.lang.String[] args, org.apache.hadoop.conf.Configuration conf) throws Exception {
        int N = Integer.parseInt(args[1].toString());
        ArrayList<java.lang.String> docs = findNBest(N, conf);

        for (java.lang.String doc : docs) {
            System.out.println(doc);
        }

        return 0;
    }

}
