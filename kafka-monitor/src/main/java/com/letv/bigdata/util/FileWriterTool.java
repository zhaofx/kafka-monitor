package com.letv.bigdata.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class FileWriterTool {
	private BufferedWriter bufferedWriter = null;

	public FileWriterTool(String logName) throws IOException {
		bufferedWriter = new BufferedWriter(new FileWriter(logName));
	}

	int count = 0;

	public void writeStrings(String logContent) throws IOException {
		bufferedWriter.write(logContent);
		if (count % 1000 == 0)
			bufferedWriter.flush();
	}

	public void close() {
		if (bufferedWriter != null) {
			try {
				bufferedWriter.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {

	}

}
