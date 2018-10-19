package edu.ucr.cs.cs226.candr006;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * HDFSUpload class
 * This class takes a path to a local input file and creates a copy in the given hdfs path
 */
public class HDFSUpload 
{

    public static void main( String[] args ) throws IOException, URISyntaxException {
    	if(args.length<3){
        	System.out.println("\n\nERROR: You are missing one or more arguments. Exiting.\n");
        	return;
    	}
    	String str_local_file=args[1];
    	String str_hdfs_path=args[2];

    	//check if the local file exists
    	File localFile= new File(str_local_file);
		if(!localFile.exists()){
			System.out.println("\n\nERROR: The local file you entered does not exist. Exiting.\n");
			return;
		}

		
		Configuration con= new Configuration();
		FileSystem fs = FileSystem.get(con);
		Path hdfsPath = new Path(str_hdfs_path);

		//check if the file in hdfs exists already
	    if(fs.exists(hdfsPath)) {
	      System.out.println("\n\nERROR: The hdfs file path you entered already exists. Exiting.\n");
	      return;
		}else{
	        System.out.println("\nFile doesn't exist!\n");
        }


    }
}
