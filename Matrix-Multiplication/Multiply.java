import java.io.IOException;
import java.util.ArrayList;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.Configuration;



class Elem implements Writable {

	short tag;  // 0 for M, 1 for   N
	int index;  // one of the indexes (the other is used as a key)
	double value;
	
	Elem() {}
	
	Elem ( short t, int i, double v ) {
        	tag = t; index=i; value=v;
    	}

    	public void write ( DataOutput out ) throws IOException {
		//System.out.println("******** Inside Write ***********");
        	out.writeShort(tag);
        	out.writeInt(index);
        	out.writeDouble(value);
    	}

    	public void readFields ( DataInput in ) throws IOException {
		//System.out.println("******** Inside ReadFields ***********");
        	tag = in.readShort();
	        index = in.readInt();
        	value = in.readDouble();
    	}


}

class Pair implements WritableComparable<Pair> {
	
	int i;
	int j;
	
	Pair() {}
	
	Pair(int a, int b) {
	
		i=a;
		j=b;

	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(i);
		output.writeInt(j);
	}

	
	public void readFields(DataInput input) throws IOException {
		i = input.readInt();
		j = input.readInt();
	}

	
	public int compareTo(Pair p) {
		
		return (i > p.i ? 1 : (i < p.i ? -1 : (j > p.j ? 1 : (j < p.j ? -1 : 0))));
		
	}
	
	public String toString() {
		return i + " " + j + " ";
	}
  
}

public class Multiply {
	public static class MatrixMMapper extends Mapper<Object,Text,IntWritable,Elem > {
        	
		@Override
		public void map ( Object key, Text value, Context context )
					throws IOException, InterruptedException {

			Scanner s = new Scanner(value.toString()).useDelimiter(",");
		
			int i = s.nextInt();
			int j = s.nextInt();
			Double v = s.nextDouble();

			IntWritable keyValue = new IntWritable(j);
			Elem e = new Elem((short)(0),i,v);
			context.write(keyValue,e);
			s.close();
		}
    }

    public static class MatrixNMapper extends Mapper<Object,Text,IntWritable,Elem > {
        	
		@Override
        public void map ( Object key, Text value, Context context )
					throws IOException, InterruptedException {

            Scanner s = new Scanner(value.toString()).useDelimiter(",");
			
            int i = s.nextInt();
			int j = s.nextInt();
			Double v = s.nextDouble();
			
			IntWritable keyValue = new IntWritable(i);
			Elem e = new Elem((short)(1),j,v);
            context.write(keyValue,e);
            s.close();
        }
    }

	public static class ReducerOne extends Reducer<IntWritable,Elem, Pair, DoubleWritable> {
	
		@Override
		public void reduce(IntWritable key, Iterable<Elem> values, Context context) 
				throws IOException, InterruptedException {
			
			ArrayList<Elem> M = new ArrayList<Elem>();
			ArrayList<Elem> N = new ArrayList<Elem>();
			
			for(Elem element : values) {
				
				Elem addElem = new Elem(element.tag,element.index,element.value);
					
				if (addElem.tag == 0) {
					M.add(addElem);
				} else if(addElem.tag == 1) {
					N.add(addElem);
				}
			}
					
			for(int i=0;i<M.size();i++) {
				for(int j=0;j<N.size();j++) {
					
					Pair p = new Pair(M.get(i).index,N.get(j).index);
					double multMxN = M.get(i).value * N.get(j).value;

					context.write(p, new DoubleWritable(multMxN));
				}
			}
		}
	}


	public static class MapperMN extends Mapper<Object, DoubleWritable, Pair, DoubleWritable> {
		
		public void map(Object key, Iterable<DoubleWritable> values, Context context) 
				throws IOException, InterruptedException {

			Scanner s = new Scanner(values.toString()).useDelimiter(" ");
			
            String i = s.next();
			String j = s.next();
			String v = s.next();
			
			Pair p = new Pair(Integer.parseInt(i),Integer.parseInt(j));
			DoubleWritable value = new DoubleWritable(Double.parseDouble(v));

			context.write(p, value);
			s.close();
		}
	}
	
	public static class ReducerMN extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
			
			double finalMxN = 0.0;
			for(DoubleWritable value : values) {
				finalMxN += value.get();
			}

			context.write(key, new DoubleWritable(finalMxN));
		}
	}


	public static void main ( String[] args ) throws Exception {
		
    	//First Mapper and Reducer
		Job job = Job.getInstance();
        job.setJobName("MultiplyIntermediateJob");
        job.setJarByClass(Multiply.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Elem.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setReducerClass(ReducerOne.class);
		
		//Generate Intermediate Binary Output
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MatrixMMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MatrixNMapper.class);
		
		FileOutputFormat.setOutputPath(job,new Path(args[2]));
       	job.waitForCompletion(true);
		
		//Second Mapper and Reducer
		job = Job.getInstance();
		job.setJobName("MultiplyJob");
		job.setJarByClass(Multiply.class);
		
		job.setMapperClass(MapperMN.class);
		job.setReducerClass(ReducerMN.class);
		
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//Read Intermediate Binary Output
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		SequenceFileInputFormat.setInputPaths(job,new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
