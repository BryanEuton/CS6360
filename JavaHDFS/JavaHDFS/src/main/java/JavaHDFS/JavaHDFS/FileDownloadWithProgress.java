package JavaHDFS.JavaHDFS;
// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

// vv FileCopyWithProgress
public class FileDownloadWithProgress {
  public static void main(String[] args) throws Exception {
    String dst = args[1];
    
    InputStream in = null;
    try{
    	in = new URL(args[0]).openStream();
        
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
          public void progress() {
            System.out.print(".");
          }
        });
        
        IOUtils.copyBytes(in, out, 4096, true);    	
    } finally {
        IOUtils.closeStream(in);
    }
  }
}
// ^^ FileCopyWithProgress
