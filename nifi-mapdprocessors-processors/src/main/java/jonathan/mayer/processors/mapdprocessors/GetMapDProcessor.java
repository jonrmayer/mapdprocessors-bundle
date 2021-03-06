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
import org.apache.nifi.util.StopWatch;
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
import com.mapd.thrift.server.TQueryResult;

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
import java.util.concurrent.TimeUnit;
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
public class GetMapDProcessor extends AbstractProcessor {

	public static final PropertyDescriptor MAPD_SERVICE = new PropertyDescriptor.Builder().name("MAPD Service")
			.description(
					"The Controller Service that is used to obtain connection to database and create a Message Bus")
			.required(true).identifiesControllerService(MapDService.class).build();

	public static final PropertyDescriptor SQL_STATEMENT = new PropertyDescriptor.Builder().name("SQL_STATEMENT")
			.displayName("SQL_STATEMENT").description("SQL_STATEMENT").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	// public static final PropertyDescriptor BUFFER_SIZE = new
	// PropertyDescriptor.Builder().name("BUFFER_SIZE")
	// .displayName("BUFFER_SIZE").description("BUFFER_SIZE").required(true)
	// .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("A FlowFile is routed to this relationship after it has been converted to JSON").build();
	static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description(
			"A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
			.build();

	private List<PropertyDescriptor> properties;

	private Set<Relationship> relationships;

	private static AtomicLong written;
	private static String err;
	public static final String RESULT_ROW_COUNT = "executesql.row.count";
    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";

	@Override
	protected void init(final ProcessorInitializationContext context) {
		super.init(context);

		final List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(MAPD_SERVICE);
		properties.add(SQL_STATEMENT);
		// properties.add(BUFFER_SIZE);

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
		FlowFile fileToProcess = null;
		if (context.hasIncomingConnection()) {
			fileToProcess = session.get();

		}
		final StopWatch stopWatch = new StopWatch(true);
		written = new AtomicLong(0L);

		final String sql_statement = context.getProperty(SQL_STATEMENT).getValue();

		// final int bufferSize = context.getProperty(BUFFER_SIZE).asInteger();
		final MapDService mapdService = context.getProperty(MAPD_SERVICE).asControllerService(MapDService.class);

		final MapD.Client client = mapdService.GetMapDClient();

		final String mapdSession = mapdService.MapDSession();

		final MapDConversionUtilities mcu = new MapDConversionUtilities(client, mapdSession);

		// FlowFile outgoingAvro = session.clone(flowFile);

		try {
			FlowFile resultSetFF;
			if (fileToProcess == null) {
				resultSetFF = session.create();
			} else {
				resultSetFF = session.create(fileToProcess);
				resultSetFF = session.putAllAttributes(resultSetFF, fileToProcess.getAttributes());
			}
			final AtomicLong nrOfRows = new AtomicLong(0L);
			resultSetFF = session.write(resultSetFF, new OutputStreamCallback() {
				@Override
				public void process(final OutputStream out) throws IOException {
					try {

						TQueryResult resultSet = mcu.GetData(sql_statement);
						final MapDCommon.AvroConversionOptions options = MapDCommon.AvroConversionOptions.builder()
								.convertNames(false).useLogicalTypes(true).defaultPrecision(10).defaultScale(0).build();
						nrOfRows.set(MapDCommon.convertToAvroStream(resultSet, out, options));
					} catch (final Exception e) {
						throw new ProcessException(e);
					}
				}
			});
			 long duration = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
			 resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
             resultSetFF = session.putAttribute(resultSetFF, RESULT_QUERY_DURATION, String.valueOf(duration));
             resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), MapDCommon.MIME_TYPE_AVRO_BINARY);
			
             session.getProvenanceReporter().modifyContent(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", duration);
             session.transfer(resultSetFF, REL_SUCCESS);
             //If we had at least one result then it's OK to drop the original file, but if we had no results then
             //  pass the original flow file down the line to trigger downstream processors
             if(fileToProcess != null){
            	 session.remove(fileToProcess);
             }

		} catch (final ProcessException pe) {

			session.transfer(fileToProcess, REL_FAILURE);

			

		}
	}

}
