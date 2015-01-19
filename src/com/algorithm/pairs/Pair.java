/**
 *
 * @author ZAUR ALIYEV
 */

package com.algorithm.pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Pair implements WritableComparable<Pair> {

	public long Firstword;
	public String Second;

	public void write(DataOutput out) throws IOException {
		out.writeLong(Firstword);
		out.writeUTF(Second);
	}

	public void readFields(DataInput in) throws IOException {
		Firstword = in.readLong();
		Second = in.readUTF();
	}

	public static Pair read(DataInput in) throws IOException {
		Pair w = new Pair();
		w.readFields(in);
		return w;
	}

	@Override
	public int compareTo(Pair obj) {
		if(obj.Firstword==this.Firstword)
		{
			return this.Second.compareTo(obj.Second);
		}
		else if(this.Firstword < obj.Firstword)
			return -1;
		else 
			return 1;
		//	return 0;

	}
	public int hashCode() {

		return  this.toString().hashCode();
	}
	@Override
	public String toString() {
		String s= Firstword+" , "+Second;
		return s;

	}

}