package edu.ucr.cs.cs226.candr006;

import java.io.*;
import java.nio.file.Paths;
import java.util.StringTokenizer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;
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
import org.apache.commons.compress.compressors.CompressorInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.BZip2Codec;

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
	      System.out.println("\n\nERROR: The hdfs path you entered already exists. Exiting.\n");
	      return;
		}else{
	        System.out.println("\nNOTE: Creating file in HDFS. Please wait...\n");
        }


        //Copy file
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
		Configuration con2= new Configuration();
		FileSystem fs2 = FileSystem.get(con2);
		Path hdfsPath2 = new Path(str_hdfs_path);
		InputStream in2 = null;
		OutputStream out2=null;
		try {
			in2 = fs.open(hdfsPath2);
			IOUtils.copyBytes(in2,out2, 4096, false);
		} finally {
			IOUtils.closeStream(in2);
		}
		in2.close();
		long estimatedTime2 = System.nanoTime() - startTime2;
		double seconds2=estimatedTime2/ 1000000000.0;
		System.out.println("Seconds it takes to read the HDFS file: "+seconds);



        //Open the local file to read from and copy to hdfs location
        /*FSDataOutputStream ostream = fs.create(hdfsPath);
        InputStream istream = new BufferedInputStream(new FileInputStream(localFile));

        byte[] byte_to_read = new byte[1024];
        int int_bytes = 0;
        while ((int_bytes = istream.read(byte_to_read)) > 0) {
            ostream.write(byte_to_read,0,int_bytes);
        }

        ostream.close();
        istream.close();*/








        fs.close();


    }
}
