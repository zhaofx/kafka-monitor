package com.letv.bigdata.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @author Administrator
 * 
 *         To change the template for this generated type comment go to Window -
 *         Preferences - Java - Code Style - Code Templates
 */
public class PropertiesUnit {
	/** log */
	private static Logger log = Logger
			.getLogger(PropertiesUnit.class.getName());
	
	private String filename = "monitor.properties";

	private Properties prop;

	private InputStream in;

	private FileOutputStream out;

	public PropertiesUnit() {
		init(filename);
	}

	public PropertiesUnit(String filename) {
		this.filename = filename;
		init(filename);
	}

	public void init(String filename) {
		try {
			in = getResourceAsStream(filename);
			prop = new Properties();
			prop.load(in);
			in.close();
		} catch (FileNotFoundException e) {
			log.error("", e);
		} catch (IOException e) {
			log.error("", e);
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				log.error("", e);
			}
		}
	}

	public void list() {
		prop.list(System.out);
	}

	public String getValue(String itemName) {
		return prop.getProperty(itemName);
	}

	public String getValue(String itemName, String defaultValue) {
		return prop.getProperty(itemName, defaultValue);
	}

	public void setValue(String itemName, String value) {
		prop.setProperty(itemName, value);
	}

	public void saveFile(String filename, String description) throws Exception {
		String CLASSPATH = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		try {
			File f = new File(CLASSPATH + filename);
			log.debug("save config file:" + f.getAbsolutePath());
			out = new FileOutputStream(f);
			prop.store(out, description);

		} catch (IOException ex) {
			log.error("",ex);
			throw new Exception("无法保存指定的配置文件:" + filename);
		} finally {
			out.close();
		}
	}

	public void saveFile(String filename) throws Exception {
		saveFile(filename, "");
	}

	public void saveFile() throws Exception {
		if (filename.length() == 0)
			throw new Exception("需指定保存的配置文件名");
		saveFile(filename);
	}

	public void deleteValue(String value) {
		prop.remove(value);
	}

	/**
	 * 从类路径找文件名,如果找不到，则从本地文件中取出流
	 * 
	 * @param fileName 文件名
	 * @return InputStream
	 */
	  public InputStream getResourceAsStream(String fileName) {
          if (fileName == null) {
                return null ;
         }
         InputStream in =  Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
          if(in == null){
               in = PropertiesUnit.class .getResourceAsStream(fileName);
         }
          if (in == null) {
               in = PropertiesUnit. class.getResourceAsStream("/" + fileName);
         }
          if (in == null) {
               in = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
         }
          if (in == null) {
               in = Thread.currentThread().getContextClassLoader().getResourceAsStream( "/" + fileName);
         }
          if (in == null) {
                try {
                     in = new FileInputStream(fileName);
               } catch (FileNotFoundException ex) {
                     System. out.println(fileName + ": " + ex.getMessage());
               }
         }
          
          if(in == null){ //从类路径下找
               String classpath = Thread.currentThread().getContextClassLoader().getResource( "").getPath();
               System.out.println("----classPath :" + classpath);
                try {
                     in = new FileInputStream(classpath + fileName);
                 } catch (Exception e) {
                     log.error("input stream error : " , e);
              }
         }
          return in;

   }

}
