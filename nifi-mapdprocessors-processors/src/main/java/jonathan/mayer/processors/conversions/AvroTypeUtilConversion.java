package jonathan.mayer.processors.conversions;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTypeUtilConversion {
	private static final Logger LOG = LoggerFactory.getLogger(AvroTypeUtilConversion.class);

	private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1);
	private static TimeZone localTimeZone = Calendar.getInstance().getTimeZone();

	public static final String SCHEMA_PATH_SEPARATOR = ".";

	public static final String LOGICAL_TYPE_ATTR_SCALE = "scale";
	public static final String LOGICAL_TYPE_ATTR_PRECISION = "precision";

	public static final String LOGICAL_TYPE = "logicalType";
	public static final String LOGICAL_TYPE_DECIMAL = "decimal";
	public static final String LOGICAL_TYPE_DATE = "date";
	public static final String LOGICAL_TYPE_TIME_MILLIS = "time-millis";
	public static final String LOGICAL_TYPE_TIME_MICROS = "time-micros";
	public static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
	public static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";

	
	static final String FIELD_ATTRIBUTE_TYPE = "avro.type";
	private AvroTypeUtilConversion() {
	}
//	public Field.Type
	public static Field avroToField(Schema schema, Object value) {
		if (schema.getType() == Schema.Type.UNION) {
			List<Schema> unionTypes = schema.getTypes();

			// Special case for unions of [null, actual type]
			if (unionTypes.size() == 2 && unionTypes.get(0).getType() == Schema.Type.NULL && value == null) {
				return Field.create(getFieldType(unionTypes.get(1)), null);
			}

			// // By default try to resolve index of the union bby the data itself
			 int typeIndex = GenericData.get().resolveUnion(schema, value);
			 schema = unionTypes.get(typeIndex);
			// record.getHeader().setAttribute(AVRO_UNION_TYPE_INDEX_PREFIX + fieldPath,
			 String.valueOf(typeIndex);
		}
		if (value == null) {
			return Field.create(getFieldType(schema), null);
		}
		Field f = null;

		String logicalType = schema.getProp(LOGICAL_TYPE);
		if (logicalType != null && !logicalType.isEmpty()) {
			Field returnField = null;
			switch (logicalType) {
			case LOGICAL_TYPE_DECIMAL:
				if (schema.getType() != Schema.Type.BYTES) {
					throw new IllegalStateException(
							"Unexpected physical type for logical decimal type: " + schema.getType());
				}
				int scale = schema.getJsonProp(LOGICAL_TYPE_ATTR_SCALE).asInt();
				int precision = schema.getJsonProp(LOGICAL_TYPE_ATTR_PRECISION).asInt();
				if (value instanceof ByteBuffer) {
					byte[] decimalBytes = ((ByteBuffer) value).array();
					value = bigDecimalFromBytes(decimalBytes, scale);
				}
				returnField = Field.create(Field.Type.DECIMAL, value);
				// returnField.setAttribute(HeaderAttributeConstants.ATTR_SCALE,
				// String.valueOf(scale));
				// returnField.setAttribute(HeaderAttributeConstants.ATTR_PRECISION,
				// String.valueOf(precision));
				break;
			case LOGICAL_TYPE_DATE:
				if (schema.getType() != Schema.Type.INT) {
					throw new IllegalStateException(
							"Unexpected physical type for logical date type: " + schema.getType());
				}
				if (value instanceof Integer) {
					// Convert days in integer since epoch to millis
					long millis = daysToMillis((int) value);
					value = new Date(millis);
				}
				returnField = Field.create(Field.Type.DATE, value);
				break;
			case LOGICAL_TYPE_TIME_MILLIS:
				if (schema.getType() != Schema.Type.INT) {
					throw new IllegalStateException(
							"Unexpected physical type for logical time millis type: " + schema.getType());
				}

				returnField = Field.create(Field.Type.TIME, (long) (int) value);
				break;
			case LOGICAL_TYPE_TIME_MICROS:
				if (schema.getType() != Schema.Type.LONG) {
					throw new IllegalStateException(
							"Unexpected physical type for logical time micros type: " + schema.getType());
				}
				// We don't have a better type to represent microseconds
				returnField = Field.create(Field.Type.LONG, value);
				break;
			case LOGICAL_TYPE_TIMESTAMP_MILLIS:
				if (schema.getType() != Schema.Type.LONG) {
					throw new IllegalStateException(
							"Unexpected physical type for logical timestamp millis type: " + schema.getType());
				}
				returnField = Field.create(Field.Type.DATETIME, value);
				break;
			case LOGICAL_TYPE_TIMESTAMP_MICROS:
				if (schema.getType() != Schema.Type.LONG) {
					throw new IllegalStateException(
							"Unexpected physical type for logical timestamp micros type: " + schema.getType());
				}
				// We don't have a better type to represent microseconds
				returnField = Field.create(Field.Type.LONG, value);
				break;
			}
			
			if(returnField != null) {
		        returnField.setAttribute(FIELD_ATTRIBUTE_TYPE, logicalType);
		        return returnField;
		      }
		}
		
		// Primitive types
	    switch(schema.getType()) {
//	      case ARRAY:
//	        List<?> objectList = (List<?>) value;
//	        List<Field> list = new ArrayList<>(objectList.size());
//	        for (int i = 0; i < objectList.size(); i++) {
//	          list.add(avroToSdcField(record, fieldPath + "[" + i + "]", schema.getElementType(), objectList.get(i)));
//	        }
//	        f = Field.create(list);
//	        break;
	      case BOOLEAN:
	        f = Field.create(Field.Type.BOOLEAN, value);
	        break;
	      case BYTES:
	        f = Field.create(Field.Type.BYTE_ARRAY, ((ByteBuffer)value).array());
	        break;
	      case DOUBLE:
	        f = Field.create(Field.Type.DOUBLE, value);
	        break;
	      case ENUM:
	        f = Field.create(Field.Type.STRING, value);
	        break;
	      case FIXED:
	        f = Field.create(Field.Type.BYTE_ARRAY, ((GenericFixed)value).bytes());
	        break;
	      case FLOAT:
	        f = Field.create(Field.Type.FLOAT, value);
	        break;
	      case INT:
	        f = Field.create(Field.Type.INTEGER, value);
	        break;
	      case LONG:
	        f = Field.create(Field.Type.LONG, value);
	        break;
//	      case MAP:
//	        Map<Object, Object> avroMap = (Map<Object, Object>) value;
//	        Map<String, Field> map = new LinkedHashMap<>();
//	        for (Map.Entry<Object, Object> entry : avroMap.entrySet()) {
//	          String key;
//	          if (entry.getKey() instanceof Utf8) {
//	            key = entry.getKey().toString();
//	          } else if (entry.getKey() instanceof String) {
//	            key = (String) entry.getKey();
//	          } else {
//	            throw new IllegalStateException(Utils.format("Unrecognized type for avro value: {}", entry.getKey()
//	                .getClass().getName()));
//	          }
//	          map.put(key, avroToSdcField(record, fieldPath + FORWARD_SLASH + key,
//	              schema.getValueType(), entry.getValue()));
//	        }
//	        f = Field.create(map);
//	        break;
	      case NULL:
	        f = Field.create(Field.Type.MAP, null);
	        break;
//	      case RECORD:
//	        GenericRecord avroRecord = (GenericRecord) value;
//	        Map<String, Field> recordMap = new HashMap<>();
//	        for(Schema.Field field : schema.getFields()) {
//	          Field temp = avroToSdcField(record, fieldPath + FORWARD_SLASH + field.name(), field.schema(),
//	              avroRecord.get(field.name()));
//	          if(temp != null) {
//	            recordMap.put(field.name(), temp);
//	          }
//	        }
//	        f = Field.create(recordMap);
//	        break;
	      case STRING:
	        f = Field.create(Field.Type.STRING, value.toString());
	        break;
	      default:
	        throw new IllegalStateException("Unexpected schema type " + schema.getType());
	    }
		return f;
	}

	

	

	public static Field.Type getFieldType(Schema schema) {
		String logicalType = schema.getProp(LOGICAL_TYPE);
		if (logicalType != null && !logicalType.isEmpty()) {
			switch (logicalType) {
			case LOGICAL_TYPE_DECIMAL:
				return Field.Type.DECIMAL;
			case LOGICAL_TYPE_DATE:
				return Field.Type.DATE;
			case LOGICAL_TYPE_TIME_MILLIS:
				return Field.Type.TIME;
			case LOGICAL_TYPE_TIME_MICROS:
				return Field.Type.LONG;
			case LOGICAL_TYPE_TIMESTAMP_MILLIS:
				return Field.Type.DATETIME;
			case LOGICAL_TYPE_TIMESTAMP_MICROS:
				return Field.Type.LONG;
			}
		}

		switch (schema.getType()) {
		case ARRAY:
			return Field.Type.LIST;
		case BOOLEAN:
			return Field.Type.BOOLEAN;
		case BYTES:
			return Field.Type.BYTE_ARRAY;
		case DOUBLE:
			return Field.Type.DOUBLE;
		case ENUM:
			return Field.Type.STRING;
		case FIXED:
			return Field.Type.BYTE_ARRAY;
		case FLOAT:
			return Field.Type.FLOAT;
		case INT:
			return Field.Type.INTEGER;
		case LONG:
			return Field.Type.LONG;
		case MAP:
			return Field.Type.MAP;
		case NULL:
			return Field.Type.MAP;
		case RECORD:
			return Field.Type.MAP;
		case STRING:
			return Field.Type.STRING;
		default:
			throw new IllegalStateException(Utils.format("Unexpected schema type {}", schema.getType().getName()));
		}
	}

	public static BigDecimal bigDecimalFromBytes(byte[] decimalBytes, int scale) {
		final BigInteger bigInt = new BigInteger(decimalBytes);
		return new BigDecimal(bigInt, scale);
	}

	public static long daysToMillis(int days) {
		long millisUtc = (long) days * MILLIS_PER_DAY;
		long tmp = millisUtc - (long) (localTimeZone.getOffset(millisUtc));
		return millisUtc - (long) (localTimeZone.getOffset(tmp));
	}
}
