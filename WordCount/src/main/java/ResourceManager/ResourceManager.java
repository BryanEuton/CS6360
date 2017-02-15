package ResourceManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ResourceManager {
	  public HashSet<String> GetResource(String name) throws IOException{
		  HashSet<String> hash= new HashSet<String>();
		  InputStream input = getClass().getResourceAsStream(name);
		  BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
			String s;
			while((s= bufferedReader.readLine()) != null) {
				if(!s.startsWith(";") && !s.trim().isEmpty()){
	        	  hash.add(s.trim().toLowerCase());
	          }
			}
			return hash;
	  }  
}