package JavaHDFS.JavaHDFS;
// cc FileDownloadAndDecompressWithProgress Downloads a file and decompresses if necessary and then outputs to a Hadoop filesystem, and shows progress
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

// vv FileDownloadAndDecompressWithProgress
public class FileDownloadAndDecompressWithProgress {
  public static void main(String[] args) throws Exception {
	String uri = args[0];
	String dst = args[1];
    
    Configuration conf = new Configuration();
    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
    FileSystem fs = FileSystem.get(URI.create(dst), conf);
    getAndDecompress(uri, dst, conf, fs);
  }
  public static void getAndDecompress(String uri, String dst, Configuration conf, FileSystem fs) throws Exception {	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    InputStream in = null;
	    try{
	    	in = new URL(uri).openStream();
		    if (codec == null) {
		    	if(uri.endsWith(".zip")){
		    		unzip(fs, in, dst, uri, conf);
		    	}else{
		    		writeFile(fs, dst, in, conf, true);
		    	}
		    }else{
		    	writeFile(fs, dst, codec.createInputStream(in), conf, true);	    	
		    }	
	    }finally{
	    	IOUtils.closeStream(in);
	    }
	  }
	  
	  private static void unzip(FileSystem fs, InputStream in, String destFolder, String uri, Configuration conf) throws Exception {
		 ZipFile zipFile = null;
		 ZipInputStream zipInputStream = null;
		 try{
			 zipInputStream = new ZipInputStream(in);
			 while(true){
				 ZipEntry zipEntry = zipInputStream.getNextEntry();
				 if(zipEntry == null){
					break;
				 }
				 
				Path destination =  new Path(destFolder, zipEntry.getName());
				if (zipEntry.isDirectory())
				{
				    if(!fs.exists(destination)){
				    	fs.mkdirs(destination);				    	
				    }				    
				}
				else
				{					
					writeFile(fs, destination.toString(), zipInputStream, conf, false);  
				}
				zipInputStream.closeEntry();
				System.out.println("Entry created - " + destination);
			 }			 
		 }
		 finally{
			 if(zipFile != null){
				 zipFile.close();
			 }
			 IOUtils.closeStream(zipInputStream);			 
		 }
	  }
	  
	  private static void writeFile(FileSystem fs, String destination, InputStream in, Configuration conf, boolean closeStream) throws Exception {
		OutputStream out = null;
		try {
		  out = fs.create(new Path(destination), new Progressable() {
	          public void progress() {
	              System.out.print(".");
	            }
	          });
		  IOUtils.copyBytes(in, out, conf, closeStream);
		} finally {
		  if(closeStream){
			  IOUtils.closeStream(in);
		  }
		  IOUtils.closeStream(out);
		}  
	  }
}
// ^^ FileDownloadAndDecompressWithProgress
