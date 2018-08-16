package jonathan.mayer.processors.mapdprocessors;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import jonathan.mayer.processors.conversions.AvroTypeUtilConversion;
public class BinaryObjectUtils {
	public static Object convertToBinaryObject(String fieldName,Schema fieldSchema, Object value) throws ClassNotFoundException
	{
		Object result = null;
		jonathan.mayer.processors.conversions.Field field = AvroTypeUtilConversion.avroToField(fieldSchema, value);
		result = convertToBinaryObject(field);

		return result;
	}
	public static Object convertToBinaryObject(jonathan.mayer.processors.conversions.Field field)
			throws ClassNotFoundException {
		Object result = null;

		jonathan.mayer.processors.conversions.Field.Type fieldType = field.getType();
		switch(fieldType) {
		case BOOLEAN:
			result = field.getValueAsBoolean();
			break;
		case BYTE:
			result = field.getValueAsByte();
			break;
		case BYTE_ARRAY:
			result = field.getValueAsByteArray();
			break;
		case CHAR:
			result = field.getValueAsChar();
			break;
		case DATE:
			result = field.getValueAsDate();
			break;
		case DATETIME:
			result = field.getValueAsDatetime();
			break;
		case DECIMAL:
			result = field.getValueAsDecimal();
			break;
		case DOUBLE:
			result = field.getValueAsDouble();
		case FLOAT:
			result = field.getValueAsFloat();
		case INTEGER:
			result = field.getValueAsDouble();
		case LONG:
			result = field.getValueAsLong();
		case SHORT:
			result = field.getValueAsShort();
			break;
		case STRING:
			result = field.getValueAsString();
			break;
		case TIME:
			result = field.getValueAsTime();
			break;
		}
		
		
		
		
		return result;
	}

}
