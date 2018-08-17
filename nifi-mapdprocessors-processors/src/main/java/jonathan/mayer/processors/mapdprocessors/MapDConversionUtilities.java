package jonathan.mayer.processors.mapdprocessors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.pow;
import static java.lang.System.exit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.thrift.TException;

import com.mapd.thrift.server.MapD;
import com.mapd.thrift.server.TColumn;
import com.mapd.thrift.server.TColumnData;
import com.mapd.thrift.server.TMapDException;
import com.mapd.thrift.server.TQueryResult;

import jonathan.mayer.processors.conversions.AvroTypeUtilConversion;
import jonathan.mayer.processors.conversions.Field.Type;




public class MapDConversionUtilities {

	private List<Field> avrofields;
	private InputStream rawIn;
	private List<TColumn> cols;
	
	
	private MapD.Client mapdclient;
	
	private String mapdsession;
	
	private int bufferSize;

	public MapDConversionUtilities() {
	}
public MapDConversionUtilities(MapD.Client _mapdclient,String _mapdsession) {
		
		mapdclient = _mapdclient;
		mapdsession = _mapdsession;
		
	}
	public MapDConversionUtilities(MapD.Client _mapdclient,String _mapdsession, int _bufferSize) {
		
		mapdclient = _mapdclient;
		mapdsession = _mapdsession;
		bufferSize = _bufferSize;
	}

	
	
	 private String getColType(int cType, int precision, int scale) {
		    if (precision > 19) {
		      precision = 19;
		    }
		    if (scale > 19) {
		      scale = 18;
		    }
		    switch (cType) {
		      case java.sql.Types.TINYINT:
		        return ("TINYINT");
		      case java.sql.Types.SMALLINT:
		        return ("SMALLINT");
		      case java.sql.Types.INTEGER:
		        return ("INTEGER");
		      case java.sql.Types.BIGINT:
		        return ("BIGINT");
		      case java.sql.Types.FLOAT:
		        return ("FLOAT");
		      case java.sql.Types.DECIMAL:
		        return ("DECIMAL(" + precision + "," + scale + ")");
		      case java.sql.Types.DOUBLE:
		        return ("DOUBLE");
		      case java.sql.Types.REAL:
		        return ("REAL");
		      case java.sql.Types.NUMERIC:
		        return ("NUMERIC(" + precision + "," + scale + ")");
		      case java.sql.Types.TIME:
		        return ("TIME");
		      case java.sql.Types.TIMESTAMP:
		        return ("TIMESTAMP");
		      case java.sql.Types.DATE:
		        return ("DATE");
		      case java.sql.Types.BOOLEAN:
		      case java.sql.Types.BIT:  // deal with postgress treating boolean as bit... this will bite me
		        return ("BOOLEAN");
		      case java.sql.Types.NVARCHAR:
		      case java.sql.Types.VARCHAR:
		      case java.sql.Types.NCHAR:
		      case java.sql.Types.CHAR:
		      case java.sql.Types.LONGVARCHAR:
		      case java.sql.Types.LONGNVARCHAR:
		        return ("TEXT ENCODING DICT");
		      default:
		        throw new AssertionError("Column type " + cType + " not Supported");
		    }
		  }
	
	
	
	
	public void resetBinaryColumns() {
		for (int i = 0; i < avrofields.size(); i++) {
			Schema.Field field = avrofields.get(i);
			Schema fieldSchema = field.schema();

			Type fieldType = GetAvroType(fieldSchema);
			TColumn col = cols.get(i);
			resetBinaryColumn(fieldType, col);
		}
	}

	public void resetBinaryColumn(Type datatype, TColumn col) {
		col.nulls.clear();
		switch (datatype) {
		case INTEGER:
		case DECIMAL:
		case BOOLEAN:
		case DATE:
		case DATETIME:
		case TIME:
		case ZONED_DATETIME:
		case SHORT:
			col.data.int_col.clear();
			break;
		case FLOAT:
		case DOUBLE:
			col.data.real_col.clear();
			break;
		case STRING:
			col.data.str_col.clear();
			break;
		default:
			throw new AssertionError("Avro Column type " + datatype + " not Supported");

		}
	}

	public List<TColumn> SetUpBinaryColumns() {

		cols = new ArrayList(avrofields.size());

		for (int i = 0; i < avrofields.size(); i++) {
			Schema.Field field = avrofields.get(i);
			Schema fieldSchema = field.schema();
			String fieldName = field.name();
			Type fieldType = GetAvroType(fieldSchema);
			TColumn col = SetUpBinaryColumn(fieldType, bufferSize);
			cols.add(col);
		}

		return cols;
	}

	public TColumn SetUpBinaryColumn(Type datatype, int bufferSize) {
		TColumn col = new TColumn();
		col.nulls = new ArrayList<Boolean>(bufferSize);
		col.data = new TColumnData();
		switch (datatype) {
		case INTEGER:
		case DECIMAL:
		case BOOLEAN:
		case DATE:
		case DATETIME:
		case TIME:
		case ZONED_DATETIME:
		case SHORT:
			col.data.int_col = new ArrayList<Long>(bufferSize);
			break;
		case FLOAT:
		case DOUBLE:
			col.data.real_col = new ArrayList<Double>(bufferSize);
			break;
		case STRING:
			
			col.data.str_col = new ArrayList<String>(bufferSize);
			break;
		default:
			throw new AssertionError("Avro Column type " + datatype + " not Supported");

		}
		return col;
	}

	public Type GetAvroType(Schema fieldSchema) {

		Type result = null;

		if (fieldSchema.getType() == Schema.Type.UNION) {
			List<Schema> unionTypes = fieldSchema.getTypes();

			if (unionTypes.size() == 2 && unionTypes.get(0).getType() == Schema.Type.NULL) {
				result = AvroTypeUtilConversion.getFieldType(unionTypes.get(1));
			}

		} else {

			result = AvroTypeUtilConversion.getFieldType(fieldSchema);
		}

		return result;

	}

	public void SetColumnValue(GenericRecord rec) throws ClassNotFoundException {
		for (int i = 0; i < avrofields.size(); i++) {
			Schema.Field field = avrofields.get(i);
			Schema fieldSchema = field.schema();
			String fieldName = field.name();
			Object o = rec.get(fieldName);
			jonathan.mayer.processors.conversions.Field valfield = AvroTypeUtilConversion.avroToField(fieldSchema, o);
			TColumn col = cols.get(i);

			jonathan.mayer.processors.conversions.Field.Type fieldType = valfield.getType();
			Object value = null;
			switch (fieldType) {
			case BOOLEAN:
				value = valfield.getValueAsBoolean();
				if (value != null) {
					col.nulls.add(Boolean.FALSE);
					col.data.int_col.add((boolean) value ? 1L : 0L);

				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}
				break;

			case FLOAT:
			case DOUBLE:
				value = valfield.getValueAsDouble();
				if (value != null) {
					col.data.real_col.add((Double) value);
					
					col.nulls.add(Boolean.FALSE);
					
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.real_col.add(new Double(0));
				}
				break;
			case DECIMAL:
				value = valfield.getValueAsDecimal();
				if (value == null) {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}else {
					BigDecimal bd = (BigDecimal) value;
					int scale = bd.scale();
					if(fieldSchema.getJsonProp("scale") != null) {
						scale = fieldSchema.getJsonProp("scale").asInt();	
					}
					col.nulls.add(Boolean.FALSE);
					col.data.int_col.add(bd.multiply(new BigDecimal(pow(10L, scale))).longValue());
				}
				
				
				break;
			
				
			case STRING:
				value = valfield.getValueAsString();
				if (value != null) {
					col.data.str_col.add((String) value);
					
					col.nulls.add(Boolean.FALSE);
					
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.str_col.add("");
				}
				break;
			case DATE:
				value = valfield.getValueAsDate();
				if (value != null) {
					col.data.int_col.add(((Date) value).getTime() / 1000);
					col.nulls.add(Boolean.FALSE);
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}
				break;
			case TIME:
				value = valfield.getValueAsTime();
				if (value != null) {
					col.data.int_col.add(((Date) value).getTime() / 1000);
					col.nulls.add(Boolean.FALSE);
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}
				break;
			case DATETIME:
				value = valfield.getValueAsDatetime();
				if (value != null) {
					col.data.int_col.add(((Date) value).getTime() / 1000);
					col.nulls.add(Boolean.FALSE);
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}
				break;
			case ZONED_DATETIME:
				value = valfield.getValueAsZonedDateTime();
				if (value != null) {
					col.data.int_col.add(((Date) value).getTime() / 1000);
					col.nulls.add(Boolean.FALSE);
				} else {
					col.nulls.add(Boolean.TRUE);
					col.data.int_col.add(0L);
				}
				break;
			}

		}
	}
	
	
	
	
	private TQueryResult executeMapDCommand(String sql) {
		TQueryResult sqlResult= null;

	    try {
	    	final MapDCommon.AvroConversionOptions options = MapDCommon.AvroConversionOptions
					.builder().convertNames(false).useLogicalTypes(true).defaultPrecision(10)
					.defaultScale(0).build();
	       sqlResult = mapdclient.sql_execute(mapdsession, sql + ";", true, null, -1, -1);
//	      MapDCommon.convertToAvroStream(sqlResult, options);
	      
	     
	    } catch (TMapDException ex) {
	    
	      exit(1);
	    } catch (TException ex) {
	     
	      exit(1);
	    }
		return sqlResult;
	  }
	
	
	
	public TQueryResult GetData(String sql) {
		return executeMapDCommand(sql);
		
	}
		

	public AtomicLong ProcessData(String tablename,InputStream _rawIn) throws IOException, ClassNotFoundException, TMapDException, TException {
		AtomicLong result = new AtomicLong(0L);
		
		InputStream in = new BufferedInputStream(_rawIn);
		DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>());
		Schema schema = reader.getSchema();
		avrofields = schema.getFields();
		SetUpBinaryColumns();

		GenericRecord currRecord = null;
		while (reader.hasNext()) {
			currRecord = reader.next();
			SetColumnValue(currRecord);
			result.incrementAndGet();
		}
		mapdclient.load_table_binary_columnar(mapdsession, tablename, cols); // old
//		mapdclient.sql_execute(session, query, column_format, nonce, first_n, at_most_n)
		return result;
	}

}
