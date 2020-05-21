package Hive.Parser;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class Parser extends GenericUDTF
{
	private final int TICKER = 0;
	private final int NAME = 2;
	private PrimitiveObjectInspector inputString;
	private static final Integer OUT_COLS = 2;
	private transient Object forwardColObj[] = new Object[OUT_COLS];
	
	public Parser() {};
	
	
@Override	
	public StructObjectInspector initialize(final ObjectInspector[] args) throws UDFArgumentException {
        inputString = (PrimitiveObjectInspector)args[0];
        final List<String> outputField = new ArrayList<String>(2);
        final List<ObjectInspector> outputValueFields = new ArrayList<ObjectInspector>(2);
        outputField.add("ticker");
        outputField.add("name");
        outputValueFields.add((ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        outputValueFields.add((ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		return (StructObjectInspector)ObjectInspectorFactory.getStandardStructObjectInspector(outputField, outputValueFields);
    }
	
	

   

@Override
    public void process(Object[] row) throws HiveException {
	
	String text=(String)this.inputString.getPrimitiveJavaObject(row[0]);
	if(text == null) return;
	String[] fields= text.split(",");
	String ticker= fields[TICKER];
	String sector= fields[fields.length-2];
	String industry = fields[fields.length - 1];	
	

	if (industry.equals("N/A") || ticker.equals("N/A") || sector.equals("N/A") ) {

		return;
	}
	
	
	
	if(fields[2].indexOf('"')!=-1) {
		fields[2]=fields[2].replace('"', ' ');
		fields[3]=fields[3].replace('"', ' ');
		fields[NAME]=fields[2]+fields[3];
  	
        
	}
	
	
		
	
	forwardColObj[0]= ticker;
	forwardColObj[1]= fields[NAME];
	
		
	forward(forwardColObj);
	
	
	}



   @Override
   public void close() throws HiveException {	
   }

}
    



