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
	        System.out.println("\nNOTE: Program is running. This may take a few moments. Please wait...\n");
        }
        fs.close();


		//--------------Copy file from local to local. Decompresses .bzip2 file at the same time--------------
		//NOTE: local file is assumed to be a .bzip2 file. This program will not handle other local file types.
		long startTime4 = System.nanoTime();
		//java.nio.file.Path local_file_path4 = Paths.get(str_local_file);
		FileInputStream is4 = new FileInputStream(localFile);
		//InputStream is = new BufferedInputStream(new FileInputStream(localFile));
		BZip2CompressorInputStream inputStream4 = new BZip2CompressorInputStream(is4, true);
		OutputStream ostream4 = new FileOutputStream("local_copy.csv");
		final byte[] buffer4 = new byte[8192];
		int n4 = 0;
		while ((n4 = inputStream4.read(buffer4))>0) {
			ostream4.write(buffer4, 0, n4);
		}
		ostream4.close();
		inputStream4.close();
		long estimatedTime4 = System.nanoTime() - startTime4;
		double seconds4=estimatedTime4/ 1000000000.0;
		System.out.println("1. Seconds it takes to copy the local file to local file: "+seconds4);


        //-------------Copy file from local to hdfs. Decompresses .bzip2 file at the same time----------
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
		System.out.println("2. Seconds it takes to copy the local file to hdfs: "+seconds);


		//---------------Read the local copy we created above from start to finish-------------
		long startTime5 = System.nanoTime();
		InputStream in5 = new BufferedInputStream(new FileInputStream("local_copy.csv"));
		byte[] byte_to_read5 = new byte[8195];
		int int_bytes5 = 0;
		ByteArrayOutputStream out5 = new ByteArrayOutputStream();
		while ((int_bytes5 = in5.read(byte_to_read5)) > 0) {
			out5.write( byte_to_read5, 0, int_bytes5 );
			out5.reset();
		}
		in5.close();
		out5.close();
		long estimatedTime5 = System.nanoTime() - startTime5;
		double seconds5=estimatedTime5/ 1000000000.0;
		System.out.println("3. Seconds it takes to read the Local copy: "+seconds5);


		//--------------Read the file we just copied to hdfs from start to finish-------------
		long startTime2 = System.nanoTime();
		//InputStream in2 = new BufferedInputStream(new FileInputStream(str_hdfs_path));
		FSDataInputStream in2 = fs.open(hdfsPath);
		BufferedReader buffer2=new BufferedReader(new InputStreamReader(in2));
		String out2= null;
		while ((out2=buffer2.readLine())!=null){
			//loop
		}

		in2.close();
		fs.close();
		long estimatedTime2 = System.nanoTime() - startTime2;
		double seconds2=estimatedTime2/ 1000000000.0;
		System.out.println("4. Seconds it takes to read the HDFS file: "+seconds2);



		//----------------Local copy: 2000 random seeks of 1KB----------------------
		long startTime6 = System.nanoTime();
		int i6=2000;
		int byte_offset6=0;

		int min6=0;
		//2GB position max
		File localCopyFile=new File("local_copy.csv");
		Path localCopyPath=new Path("local_copy.csv");
		int max6= (int) (fs.getFileStatus(localCopyPath).getLen());
		if(max6<0){
			max6=(-1)*max6;
		}

		while (i6>0) {
			Random rnum = new Random();
			//byte offset is a random value from 0 to 2e9 (2GB)
			byte_offset6 = rnum.nextInt((max6 - min6) + 1) + min6;
			RandomAccessFile raf = new RandomAccessFile(localCopyFile,"r");
			raf.seek(byte_offset6);
			byte[] bytes = new byte[1000];
			raf.read(bytes);
			raf.close();
			i6--;
		}

		long estimatedTime6 = System.nanoTime() - startTime6;
		double seconds6=estimatedTime6/ 1000000000.0;
		System.out.println("5. Seconds it takes to do 2000 random seeks of 1KB (Local Copy): "+seconds6);





		//-------------------HDFS Copy: 2000 random seeks of 1KB----------------------
		long startTime3 = System.nanoTime();
		int i=2000;
		int byte_offset=0;

		int min=0;
		//2GB position max
		int max= (int) (fs.getFileStatus(hdfsPath).getLen());
		if(max<0){
			max=(-1)*max;
		}
		int byte_to_read2 = 1000;
		File hdfsFile=new File(str_hdfs_path);
		FSDataInputStream in3 = fs.open(hdfsPath);

		while (i>0) {
			Random rnum = new Random();
			//byte offset is a random value from 0 to 2e9 (2GB)
			byte_offset = rnum.nextInt((max - min) + 1) + min;

			byte[] bytes = new byte[1000];
			in3.seek(byte_offset);
			in3.read(byte_offset,bytes,0, 1000);
			i--;
		}
		in3.close();
		fs.close();

		long estimatedTime3 = System.nanoTime() - startTime3;
		double seconds3=estimatedTime3/ 1000000000.0;
		System.out.println("6. Seconds it takes to do 2000 random seeks of 1KB (HDFS Copy): "+seconds3);





        fs.close();


    }
}
