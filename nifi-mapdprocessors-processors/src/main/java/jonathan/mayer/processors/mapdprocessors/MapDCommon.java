package jonathan.mayer.processors.mapdprocessors;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.function.Function;
import org.apache.nifi.util.StringUtils;

import com.mapd.thrift.server.TDatumType;
import com.mapd.thrift.server.TQueryResult;
import com.mapd.thrift.server.TTypeInfo;

public class MapDCommon {
	private static final int MAX_DIGITS_IN_BIGINT = 19;
	private static final int MAX_DIGITS_IN_INT = 9;
	// Derived from MySQL default precision.
	private static final int DEFAULT_PRECISION_VALUE = 10;
	private static final int DEFAULT_SCALE_VALUE = 0;

	// private static final int JGEOM = 2002;

	public static final String MIME_TYPE_AVRO_BINARY = "application/avro-binary";

	public static long convertToAvroStream(final TQueryResult rs, final OutputStream outStream,final AvroConversionOptions options) throws IOException {
		final Schema schema = createSchema(rs, options);
		final GenericRecord rec = new GenericData.Record(schema);
		final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
			dataFileWriter.create(schema, outStream);
			final int numCols = rs.row_set.row_desc.size();
			final int numRows = rs.row_set.columns.get(0).nulls.size();
			for (int r = 0; r < numRows; r++) {
				 for (int c = 0; c < numCols; c++) {
					 	final int decimalPrecision;
						final int decimalScale;
						final Schema fieldSchema = schema.getFields().get(c).schema();
						String fieldName = rs.row_set.row_desc.get(c).col_name;
						String columnName = options.convertNames ? normalizeNameForAvro(fieldName) : fieldName;
						TTypeInfo fieldtypeinfo = rs.row_set.row_desc.get(c).col_type;
						TDatumType fieldType = fieldtypeinfo.type;

						boolean fieldIsArray = fieldtypeinfo.is_array;
						String fieldType2 = fieldType.toString(); 
						switch (fieldType2) {
						case "TIME":
		                case "TIMESTAMP":
		                case "DATE":
		                	
		                	Date d =	new Date(rs.row_set.columns.get(c).data.int_col.get(r)*1000) ;
		                
						
						}
					 
				 }
				
			}
		}
		
		return 0;

	}

	private static void addNullableField(FieldAssembler<Schema> builder, String columnName,
			Function<BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>>, UnionAccumulator<NullDefault<Schema>>> func) {
		final BaseTypeBuilder<UnionAccumulator<NullDefault<Schema>>> and = builder.name(columnName).type().unionOf()
				.nullBuilder().endNull().and();
		func.apply(and).endUnion().noDefault();

	}

	public static Schema createSchema(final TQueryResult rs, AvroConversionOptions options) {
		String tableName = StringUtils.isEmpty(options.recordName) ? "NiFi_ExecuteMapDSQL_Record" : options.recordName;
		final int nrOfColumns = rs.row_set.row_desc.size();
		final FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
		for (int c = 0; c < nrOfColumns; c++) {
			final int decimalPrecision;
			final int decimalScale;
			String fieldName = rs.row_set.row_desc.get(c).col_name;
			String columnName = options.convertNames ? normalizeNameForAvro(fieldName) : fieldName;
			TTypeInfo fieldtypeinfo = rs.row_set.row_desc.get(c).col_type;
			TDatumType fieldType = fieldtypeinfo.type;

			boolean fieldIsArray = fieldtypeinfo.is_array;
			String fieldType2 = fieldType.toString();
			switch (fieldType2) {
			case "BOOL":
				builder.name(columnName).type().unionOf().nullBuilder().endNull().and().booleanType().endUnion()
						.noDefault();
				break;
			case "SMALLINT":
			case "TINYINT":
				builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion()
						.noDefault();
			
				break;
			case "INT":
				decimalPrecision = fieldtypeinfo.precision;
				if (decimalPrecision > 0 && decimalPrecision <= MAX_DIGITS_IN_INT) {
					builder.name(columnName).type().unionOf().nullBuilder().endNull().and().intType().endUnion()
							.noDefault();
				} else {
					builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion()
							.noDefault();
				}
				break;
				
			case "BIGINT":
				decimalPrecision = fieldtypeinfo.precision;
				if (decimalPrecision < 0 || decimalPrecision > MAX_DIGITS_IN_BIGINT) {
					builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion()
							.noDefault();
				} else {
					builder.name(columnName).type().unionOf().nullBuilder().endNull().and().longType().endUnion()
							.noDefault();
				}
				break;
			case "FLOAT":
				builder.name(columnName).type().unionOf().nullBuilder().endNull().and().floatType().endUnion()
						.noDefault();
				break;
			case "DOUBLE":
				builder.name(columnName).type().unionOf().nullBuilder().endNull().and().doubleType().endUnion()
						.noDefault();
				break;
			case "DECIMAL":
				// fieldtypeinfo
				if (options.useLogicalTypes) {
					
					if (fieldtypeinfo.precision > 0) {
						decimalPrecision = fieldtypeinfo.precision;
						decimalScale = fieldtypeinfo.scale;
					} else {
						decimalPrecision = options.defaultPrecision;
						decimalScale = fieldtypeinfo.scale > 0 ? fieldtypeinfo.scale : options.defaultScale;
					}
					final LogicalTypes.Decimal decimal = LogicalTypes.decimal(decimalPrecision, decimalScale);
					addNullableField(builder, columnName,
							u -> u.type(decimal.addToSchema(SchemaBuilder.builder().bytesType())));
				} else {
					addNullableField(builder, columnName, u -> u.stringType());
				}

				break;
			case "STR":
				builder.name(columnName).type().unionOf().nullBuilder().endNull().and().stringType().endUnion()
						.noDefault();
				break;
			case "TIME":
				addNullableField(builder, columnName,
						u -> options.useLogicalTypes
								? u.type(LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType()))
								: u.stringType());
				break;
			case "TIMESTAMP":
				addNullableField(builder, columnName,
						u -> options.useLogicalTypes
								? u.type(LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType()))
								: u.stringType());
				break;
			case "DATE":
				addNullableField(builder, columnName,
						u -> options.useLogicalTypes
								? u.type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
								: u.stringType());
				break;
			default:
				throw new IllegalArgumentException("createSchema: Unknown MapD SQL type " + fieldType2 + " / "
						+ columnName + " (table: " + tableName + ", column: " + columnName
						+ ") cannot be converted to Avro type");
			}

		}

		return builder.endRecord();

	}

	public static String normalizeNameForAvro(String inputName) {
		String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
		if (Character.isDigit(normalizedName.charAt(0))) {
			normalizedName = "_" + normalizedName;
		}
		return normalizedName;
	}

	public static class AvroConversionOptions {
		private final String recordName;
		private final int maxRows;
		private final boolean convertNames;
		private final boolean useLogicalTypes;
		private final int defaultPrecision;
		private final int defaultScale;

		private AvroConversionOptions(String recordName, int maxRows, boolean convertNames, boolean useLogicalTypes,
				int defaultPrecision, int defaultScale) {
			this.recordName = recordName;
			this.maxRows = maxRows;
			this.convertNames = convertNames;
			this.useLogicalTypes = useLogicalTypes;
			this.defaultPrecision = defaultPrecision;
			this.defaultScale = defaultScale;

		}

		public static Builder builder() {
			return new Builder();
		}

		public static class Builder {
			private String recordName;
			private int maxRows = 0;
			private boolean convertNames = false;
			private boolean useLogicalTypes = false;
			private int defaultPrecision = DEFAULT_PRECISION_VALUE;
			private int defaultScale = DEFAULT_SCALE_VALUE;

			/**
			 * Specify a priori record name to use if it cannot be determined from the
			 * result set.
			 */
			public Builder recordName(String recordName) {
				this.recordName = recordName;
				return this;
			}

			public Builder maxRows(int maxRows) {
				this.maxRows = maxRows;
				return this;
			}

			public Builder convertNames(boolean convertNames) {
				this.convertNames = convertNames;
				return this;
			}

			public Builder useLogicalTypes(boolean useLogicalTypes) {
				this.useLogicalTypes = useLogicalTypes;
				return this;
			}

			public Builder defaultPrecision(int defaultPrecision) {
				this.defaultPrecision = defaultPrecision;
				return this;
			}

			public Builder defaultScale(int defaultScale) {
				this.defaultScale = defaultScale;
				return this;
			}

			public AvroConversionOptions build() {
				return new AvroConversionOptions(recordName, maxRows, convertNames, useLogicalTypes, defaultPrecision,
						defaultScale);
			}
		}
	}

}
