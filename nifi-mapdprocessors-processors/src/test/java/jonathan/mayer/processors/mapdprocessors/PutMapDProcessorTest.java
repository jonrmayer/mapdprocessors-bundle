/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jonathan.mayer.processors.mapdprocessors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import jonathan.mayer.mapdservices.MapDClientService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.function.Function;


import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.NullDefault;
import org.apache.avro.SchemaBuilder.UnionAccumulator;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

public class PutMapDProcessorTest {

	@Before
	public void init() {

	}

	@Test
	public void testService() throws InitializationException, IOException {
		final TestRunner runner = TestRunners.newTestRunner(PutMapDProcessor.class);
		OutputStream testin = testavro();
		final MapDClientService service = new MapDClientService();
		runner.addControllerService("service", service);

		runner.setProperty(service, MapDClientService.MAPD_URL, "YOUR SERVER");
		runner.setProperty(service, MapDClientService.DB_NAME, "mapd");
		runner.setProperty(service, MapDClientService.PORT_NUMBER, "9091");
		runner.setProperty(service, MapDClientService.USER_NAME, "mapd");
		runner.setProperty(service, MapDClientService.USER_PASSWORD, "HyperInteractive");

	
		runner.enableControllerService(service);

		runner.setProperty(PutMapDProcessor.MAPD_SERVICE, "service");
		
		runner.setProperty(PutMapDProcessor.MAPD_TABLE, "TEST5");
		runner.setProperty(PutMapDProcessor.BUFFER_SIZE, "100000");

		long startTime = System.currentTimeMillis();
		ByteArrayOutputStream buffer = (ByteArrayOutputStream) testin;
		byte[] bytes = buffer.toByteArray();
		runner.enqueue(bytes);
		

		runner.run();
		long endTime = System.currentTimeMillis();
		System.out.println("That took " + (endTime - startTime) + " milliseconds");
	}
	public static OutputStream testavro() throws IOException {
		 Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
		 Schema _date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
		 Schema _decimal =  LogicalTypes.decimal(1, 1).addToSchema(Schema.create(Schema.Type.BYTES));
		 
		 
		 final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("testRecord").namespace("any.data")
				.fields();
//		CREATE TABLE  TEST(TEST_STRING TEXT ENCODING DICT,TEST_FLOAT FLOAT,TEST_DATE DATE,TEST_DECIMAL DECIMAL(10),TEST_TIMESTAMP TIMESTAMP)
		
		builder.name("TEST_STRING").type().unionOf().nullBuilder().endNull().and().stringType().endUnion().noDefault();
		
		builder.name("TEST_FLOAT").type().unionOf().nullBuilder().endNull().and().floatType().endUnion().noDefault();
		builder.name("TEST_DATE").type(_date).noDefault();
		builder.name("TEST_DECIMAL").type(_decimal).noDefault();
		builder.name("TEST_TIMESTAMP").type(_date).noDefault();



		Schema avroschema = builder.endRecord();
		final DataFileWriter<Record> writer = new DataFileWriter<>(AvroUtil.newDatumWriter(avroschema, Record.class));
		OutputStream rawIn = new ByteArrayOutputStream();
		try (DataFileWriter<Record> w = writer.create(avroschema, rawIn)) {
	String val = null;
			
			for (int i = 0; i < 100000; i++) {
				String test_string = "HELLO_" + i;
				Date date = new Date();
				long millis = System.currentTimeMillis() ;//% 1000;
				double dec = 890.0 ;
	            BigDecimal big = new BigDecimal(dec);     
				
				Object test_date = AvroTypeUtil.convertToAvroObject(date, avroschema.getFields().get(2).schema());
				Object test_decimal = AvroTypeUtil.convertToAvroObject(big, avroschema.getFields().get(3).schema());
				Object test_timestamp = AvroTypeUtil.convertToAvroObject(millis, avroschema.getFields().get(4).schema());
				Record rec = new Record(avroschema);
				float test_float = (float) i;
				rec.put("TEST_STRING", test_string);
				rec.put("TEST_FLOAT", test_float);
				rec.put("TEST_DATE", test_date);
				rec.put("TEST_DECIMAL", test_decimal);
				rec.put("TEST_TIMESTAMP", test_timestamp);


				writer.append(rec);
			}


		}

		return rawIn;
	}
	public static byte[] intToBytes(final int i) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(i);
		return bb.array();
	}

	static class AvroUtil {

		@SuppressWarnings("unchecked")
		public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
			return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
		}

		@SuppressWarnings("unchecked")
		public <D> DatumReader<D> newDatumReader(Schema schema, Class<D> dClass) {
			return (DatumReader<D>) GenericData.get().createDatumReader(schema);
		}

	}
}
