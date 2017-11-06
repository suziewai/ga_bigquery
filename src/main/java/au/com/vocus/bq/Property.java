package au.com.vocus.bq;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class Property {

	public static class PropertyKey {
		public static String DISABLED = ".diabled";
		public static String DATE_FORMAT = "tableId.date.format";
		public static String LAST_UPDATE = ".tableId.lastupdate";
	}

	private static String filename = ".bq.properties";
	private Properties PROPS = new Properties();
	private SimpleDateFormat df;
	
	public Property(String mode) {
		try {
			filename = mode + filename;
            final InputStream inputStream = new FileInputStream(filename);
            PROPS.load(inputStream);
            inputStream.close();
            
            df = new SimpleDateFormat(getDateFormat());
            
        } catch (IOException e) {
            System.out.println("Error loading properties file.");
            e.printStackTrace();
        }
	}
	
	public boolean getDisabled(String dsid) {
		return Boolean.valueOf(PROPS.getProperty(dsid+PropertyKey.DISABLED));
	}
	
	public String getLastUpdate(String dsid) {		
		return PROPS.getProperty(dsid+PropertyKey.LAST_UPDATE);
	}
		
	public void setLastUpdate(String dsid, String lastUpdate) {
		PROPS.put(dsid+PropertyKey.LAST_UPDATE, lastUpdate);
	}

	public String getDateFormat() {
		return PROPS.getProperty(PropertyKey.DATE_FORMAT);
	}
	
	public SimpleDateFormat getFormatter() {
		return df;
	}
		
	public void save() {
		try {
			PROPS.store(new FileOutputStream(filename), null);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
}
