package org.nebula.cfs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.datastax.bdp.hadoop.cfs.CassandraFileSystem;

public class CFSWriter {
	
	Configuration conf = null;
	FSDataOutputStream o = null;
	CassandraFileSystem cfs = null;
	BufferedWriter bw = null;
	FSDataOutputStream fsOut = null;

	public CFSWriter(String pathStr) {

		System.setProperty("cassandra.config",
				ClassLoader.getSystemResource("conf/cassandra.yaml").toString());
		System.setProperty("dse.config",
				ClassLoader.getSystemResource("conf/dse.yaml").toString());

		
		this.conf = new Configuration();
		this.conf.addResource(new Path("conf/core-site.xml"));
		
		Path path = new Path(pathStr);

		try {
//			this.conf.set("fs.default.name", "cfs://192.168.3.100:9160/");
			this.cfs = new CassandraFileSystem();
			this.cfs.initialize(URI.create("cfs://192.168.3.100:9160"), conf);
			this.cfs.createNewFile(path);
			this.fsOut = cfs.append(path,4048);
			System.out.println(String.format("Path %s exists? %s", path, this.cfs.exists(path)));
			this.bw = new BufferedWriter(new OutputStreamWriter(fsOut),4048);
		} catch (JsonMappingException err) {
			err.printStackTrace();
		} catch (JsonGenerationException err) {
			err.printStackTrace();
		} catch (IOException err) {
			err.printStackTrace();
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.print("Closing CFS Writer");
				close();
				System.out.println("...Closed CFS Writer");
			}
		});
		
	}
	
	public void append(String line){
		
		try {

			System.out.println(String.format("Writing: %s",line ));
			this.bw.write(line);
			this.bw.newLine();
			this.bw.flush();

		} catch (JsonMappingException err) {
			err.printStackTrace();
		} catch (JsonGenerationException err) {
			err.printStackTrace();
		} catch (IOException err) {
			err.printStackTrace();
		}
		
	}

	public void close() {

		try {
			this.bw.flush();
			this.bw.close();
			this.cfs.close();
			this.fsOut.close();
		} catch (IOException err) {
			err.printStackTrace();
		}

	}
}
