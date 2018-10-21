package edu.ucr.cs.cs226.candr006;

import java.io.*;
import java.nio.file.Paths;
import java.util.Random;
import java.util.StringTokenizer;

import net.minidev.json.JSONObject;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.commons.compress.compressors.CompressorInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;

import static java.nio.file.Files.probeContentType;


/**
 * HDFSUpload class
 * This class takes a path to a local input file and creates a copy in the given hdfs path
 */
public class HDFSUpload 
{

    public static void main( String[] args ) throws IOException, URISyntaxException {
    	if(args.length<3){
        	System.out.println("\n\nERROR: You are missing one or more arguments.");
			System.out.println("<local file path> <hdfs path>");
			System.out.println("Exiting");
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
	      System.out.println("\n\nERROR: The hdfs path you entered already exists. Exiting.\n");
	      return;
		}else{
	        System.out.println("\nNOTE: Creating file in HDFS. This may take a few moments. Please wait...\n");
        }


        //Copy file from local to hdfs. Decompresses .bzip2 file at the same time
		//NOTE: local file is assumed to be a .bzip2 file. This program will not handle other local file types.
		long startTime = System.nanoTime();
		java.nio.file.Path local_file_path = Paths.get(str_local_file);
		InputStream is = new BufferedInputStream(new FileInputStream(localFile));
		BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(is, true);
		FSDataOutputStream ostream = fs.create(hdfsPath);
        final byte[] buffer = new byte[8192];
        int n = 0;
        while ((n = inputStream.read(buffer))>0) {
            ostream.write(buffer, 0, n);
        }
        ostream.close();
        inputStream.close();
		long estimatedTime = System.nanoTime() - startTime;
		double seconds=estimatedTime/ 1000000000.0;
		System.out.println("Seconds it takes to copy the local file to hdfs: "+seconds);



		//Read the file we just copied to hdfs from start to finish
		long startTime2 = System.nanoTime();
		InputStream in2 = new BufferedInputStream(new FileInputStream(str_hdfs_path));
		byte[] byte_to_read = new byte[8192];
		int int_bytes = 0;
		ByteArrayOutputStream out2 = new ByteArrayOutputStream();
		while ((int_bytes = in2.read(byte_to_read)) > 0) {
			out2.write( byte_to_read, 0, int_bytes );
			out2.reset();
		}
		in2.close();
		out2.close();
		long estimatedTime2 = System.nanoTime() - startTime2;
		double seconds2=estimatedTime2/ 1000000000.0;
		System.out.println("Seconds it takes to read the HDFS file: "+seconds2);



		//2000 random seeks of 1KB
		long startTime3 = System.nanoTime();
		InputStream in3= new BufferedInputStream((new FileInputStream(str_hdfs_path)));
		int i=2000;
		int byte_offset=0;
		byte[] byte_to_read2 = new byte[8192];
		while (i>0) {
			//byte offset is a random value from 0 to 2e9 (2GB)
			int min=0;
			int max=((2 * (10*10*10*10*10*10*10*10*10)))-1000;
			Random rnum = new Random();
			byte_offset = rnum.nextInt((max - min) + 1) + min;
			System.out.println(byte_offset);
			in3.read(byte_to_read2, byte_offset, 1000);
			i--;
		}
		in3.close();
		long estimatedTime3 = System.nanoTime() - startTime3;
		double seconds3=estimatedTime3/ 1000000000.0;
		System.out.println("Seconds it takes to do 2000 random seeks of 1KB: "+seconds3);





        fs.close();


    }
}
