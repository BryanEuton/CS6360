package JavaHDFS.JavaHDFS;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class FileDecompressor {

	  public static void main(String[] args) throws Exception {
	    String uri = args[0];
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	    	if(uri.endsWith(".zip")){
	    		unzip(fs, inputPath, uri, conf);
	    	}else{
	  	      System.err.println("No codec found for " + uri);
		      System.exit(1);	    		
	    	}
	    }else{
	    	String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());			
		  	writeFile(fs, outputUri, codec.createInputStream(fs.open(inputPath)), conf);	    	
	    }	    
	  }
	  
	  private static void unzip(FileSystem fs, Path inputPath, String uri, Configuration conf) throws Exception {
		 String folder = inputPath.getParent().getName();
		 ZipFile zipFile = null;
		 try{
			 ZipInputStream zipInputSteam = new ZipInputStream(fs.open(inputPath));
			 while(true){
				ZipEntry zipEntry = zipInputSteam.getNextEntry();
				if(zipEntry == null){
					break;
				}
				final String destination =  Paths.get(folder, zipEntry.getName()).toString();
				System.out.println("Entry created - " + destination);
				if (zipEntry.isDirectory())
				{
				    File dir = new File(destination);
				    if(!dir.exists()){
				    	dir.mkdirs();
				    }					    
				}
				else
				{					
					writeFile(fs, destination, zipInputSteam, conf);  
				}
			 }			 
		 }
		 catch(IOException ex){
			 if(!ex.getMessage().contains("Stream closed")){
				 throw ex;
			 }
		 }
		 finally{
			 if(zipFile != null){
				 zipFile.close();
			 }
		 }
	  }
	  
	  private static void writeFile(FileSystem fs, String destination, InputStream in, Configuration conf) throws Exception {
		OutputStream out = null;
		try {
		  out = fs.create(new Path(destination));
		  IOUtils.copyBytes(in, out, conf);
		} finally {
		  IOUtils.closeStream(in);
		  IOUtils.closeStream(out);
		}  
	  }
	}
	// ^^ FileDecompressor

