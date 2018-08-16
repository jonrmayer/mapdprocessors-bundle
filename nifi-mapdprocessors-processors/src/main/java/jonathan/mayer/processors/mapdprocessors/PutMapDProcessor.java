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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.thrift.TException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;

import com.mapd.thrift.server.MapD;
import com.mapd.thrift.server.TColumn;
import com.mapd.thrift.server.TMapDException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.AbstractMap;
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import jonathan.mayer.mapdservices.*;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class PutMapDProcessor extends AbstractProcessor {

	public static final PropertyDescriptor MAPD_SERVICE = new PropertyDescriptor.Builder().name("MAPD Service")
			.description(
					"The Controller Service that is used to obtain connection to database and create a Message Bus")
			.required(true).identifiesControllerService(MapDService.class).build();

	public static final PropertyDescriptor MAPD_TABLE = new PropertyDescriptor.Builder().name("MAPD_TABLE")
			.displayName("MAPD_TABLE").description("MAPD_TABLE").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder().name("BUFFER_SIZE")
			.displayName("BUFFER_SIZE").description("BUFFER_SIZE").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("A FlowFile is routed to this relationship after it has been converted to JSON").build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
			"A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
			.build();

	private List<PropertyDescriptor> properties;

	private Set<Relationship> relationships;
	
	private static AtomicLong written;
	private static String err;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		super.init(context);

		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(MAPD_SERVICE);
		properties.add(MAPD_TABLE);
		properties.add(BUFFER_SIZE);

		// properties.add(SQLStatement);
		// properties.add(ROWID_THRESHOLD);

		this.properties = Collections.unmodifiableList(properties);
	}

	@Override
	public Set<Relationship> getRelationships() {
		final Set<Relationship> rels = new HashSet<>();
		rels.add(REL_SUCCESS);
		rels.add(REL_FAILURE);
		return rels;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		written = new AtomicLong(0L);

		final String tablename = context.getProperty(MAPD_TABLE).getValue();

		final int bufferSize = context.getProperty(BUFFER_SIZE).asInteger();
		final MapDService mapdService = context.getProperty(MAPD_SERVICE).asControllerService(MapDService.class);

		final MapD.Client client = mapdService.GetMapDClient();
		
		
		final String mapdSession = mapdService.MapDSession();
		
		
		final MapDConversionUtilities mcu = new MapDConversionUtilities(client, mapdSession,bufferSize);

		FlowFile outgoingAvro = session.clone(flowFile);
	

		try {
		
			outgoingAvro = session.write(outgoingAvro, new StreamCallback() {
				@Override
				public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
					try {
						written = mcu.ProcessData(tablename, rawIn);
					} catch (ClassNotFoundException | TException e) {
						err = e.getMessage();
						
					}
					
				}

			});
			//
			outgoingAvro = session.putAttribute(outgoingAvro, "mapd_records_written", String.valueOf(written.get()));
			session.remove(flowFile);
			session.transfer(outgoingAvro, REL_SUCCESS);

		} catch (final ProcessException pe) {

			outgoingAvro = session.putAttribute(flowFile, "mapd_error", err);
			session.remove(flowFile);
			session.transfer(outgoingAvro, REL_FAILURE);
			
			

			return;
			// }

		}
	}

}
