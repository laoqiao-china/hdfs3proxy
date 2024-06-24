package hdfs3proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test {
    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration conf=new Configuration();
        FileSystem hdfs2 = FileSystem.get(new URI("hdfs://hdfs2/a.log"), conf);
        FSDataInputStream is = hdfs2.open(new Path("hdfs://hdfs2/a.log"));
        byte[] data=is.readAllBytes();
        System.out.println(new String(data));
        is.close();

        FileSystem hdfs3 = FileSystem.get(new URI("hdfs://hdfs3"), conf);
        is = hdfs2.open(new Path("hdfs://hdfs3/3fuben.log"));
        data=is.readAllBytes();
        System.out.println(new String(data));
        is.close();

        is = hdfs2.open(new Path("hdfs://hdfs3/ec.log"));
        data=is.readAllBytes();
        System.out.println(new String(data));
        is.close();
    }
}
