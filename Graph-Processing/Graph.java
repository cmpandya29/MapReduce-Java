import java.io.*;
import java.lang.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
	
	Vertex() {}
	Vertex(short tag, long group, long VID, Vector<Long> adjacent) {
		this.tag = tag;
		this.group = group;
		this.VID = VID;
		this.adjacent = adjacent;
	}
    
	Vertex(short tag, long group) {
		this.tag = tag;
		this.group = group;
		this.adjacent = new Vector<Long>();
		this.VID = 0;
	}
	
	public void write ( DataOutput out ) throws IOException {
		
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);
		
		if (!adjacent.isEmpty()) {
			
			int size = adjacent.size();
			
			out.writeInt(adjacent.size());
		
			for(int i=0; i<size; i++) {
				out.writeLong(adjacent.get(i));
			}
		}
		else {
			out.writeInt(0);
		}
	}
	
	public void readFields ( DataInput in ) throws IOException {
		
		tag = in.readShort();
	    group = in.readLong();
        VID = in.readLong();
				
		adjacent = new Vector<Long>();
		
		int size = in.readInt();
		
		for(int i=0;i<size;i++) {
			adjacent.add(in.readLong());
		}
		
	}
}

public class Graph {

    public static class FirstMapper extends Mapper<Object,Text,LongWritable,Vertex > {
        	
		@Override
		public void map ( Object key, Text value, Context context )
					throws IOException, InterruptedException {

			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			
			Long VID = s.nextLong();
			LongWritable keyValue = new LongWritable(VID);
			
			Vertex vertex = new Vertex();
			vertex.adjacent = new Vector<Long>();
			vertex.tag = (short)0;
			vertex.group = VID;
			vertex.VID = VID;
			
			
			
			while(s.hasNext()){
				Long adj = s.nextLong();
    			vertex.adjacent.add(adj);
			}
			
						
			context.write(keyValue,vertex);
			s.close();
		}
    }
	
	public static class SecondMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex > {
        	
		@Override
		public void map ( LongWritable key, Vertex vertex, Context context )
					throws IOException, InterruptedException {
			
			context.write(new LongWritable(vertex.VID),vertex);
			
			
			Iterator<Long> adjac = vertex.adjacent.iterator();
			
			while(adjac.hasNext()) {
				long adj = adjac.next();
				
				context.write(new LongWritable(adj),new Vertex((short)1,vertex.group));
			}
			
		}
    }
	
	public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {
		@Override
		public void reduce(LongWritable key, Iterable<Vertex> vertices, Context context)
			throws IOException, InterruptedException {
			
			Long m = Long.MAX_VALUE;
			
			Vector adj = new Vector<Long>();
			for(Vertex v: vertices) {
				if(v.tag == 0) {
					adj = (Vector)v.adjacent.clone();
				}
				m = Math.min(m, v.group);
				
			}
			
			context.write(new LongWritable(m), new Vertex((short)0,m,key.get(),adj));
		}
	}

	
	public static class ThirdMapper extends Mapper<LongWritable,Vertex,LongWritable,IntWritable > {
        	
		@Override
		public void map ( LongWritable key, Vertex vertex, Context context )
				throws IOException, InterruptedException {
				
				context.write(key,new IntWritable(1));	

			
		}
    }
	
	public static class ThirdReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
		@Override
		public void reduce(LongWritable key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
			
			int m = 0;
			for(IntWritable i: counts) {
				m += i.get();
			}
			
			context.write(key, new IntWritable(m));
			
		}
	}
	
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("ReadGraphJob");
		job.setJarByClass(Graph.class);
				
        /* ... First Map-Reduce job to read the graph */
		
		job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Vertex.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Vertex.class);
		
		job.setMapperClass(FirstMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
		
		
        job.waitForCompletion(true);
		
        for ( short i = 0; i < 5; i++ ) {
            job = Job.getInstance();
			job.setJobName("ProcessGraphJob");
			job.setJarByClass(Graph.class);
			
            /* ... Second Map-Reduce job to propagate the group number */
			
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Vertex.class);
	    
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Vertex.class);
			
			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+i));
			SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
		job.setJobName("GroupingJob");
		job.setJarByClass(Graph.class);
        /* ... Final Map-Reduce job to calculate the connected component sizes */
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);
	
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(ThirdMapper.class);
		job.setReducerClass(ThirdReducer.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/f5"));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
        job.waitForCompletion(true);
    }
}
