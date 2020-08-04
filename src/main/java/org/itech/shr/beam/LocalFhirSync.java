/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// mvn clean package -DskipTests -P flink-runner
// --output=/tmp/patient.txt --runner=FlinkRunner
// org.itech.shr.beam.LocalFhirSync

package org.itech.shr.beam;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.model.api.IResource;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.gclient.ICriterion;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.gclient.IUntypedQuery;
import ca.uhn.fhir.rest.gclient.ReferenceClientParam;
import lombok.SneakyThrows;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.r4.model.AllergyIntolerance;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Element;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.ServiceRequest;
import org.itech.shr.beam.httpio.HttpWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *
 * <p>To execute this pipeline, specify a local output file (if using the {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * ShakespearWordCounte. You can override it and choose your own input with {@code --inputFile}.
 */
public class LocalFhirSync {

	// Create a context
	private static FhirContext ctx = FhirContext.forR4();
	private static String SOURCE_URL;
	private static String DEST_URL;
	private static List<Class> RESOURCES = Arrays.asList(Encounter.class, Condition.class, DiagnosticReport.class, ServiceRequest.class, AllergyIntolerance.class);


	// Bundle resp = client.transaction().withBundle(patientBundle).execute();

	// Transforms

	// 1. Read in patient list
	static class ReadPatients extends PTransform<PBegin, PCollection<KV<String, Bundle>>> {

		private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);

		@Override
		public PCollection<KV<String, Bundle>> expand(PBegin input) {
			// Create a client
			IGenericClient client = getSourceClient();

			PCollection<Patient> patientPCollection;
			PCollection<Class> resourcePCollection;
			PCollection<KV<String, Bundle>> patientResources;

			ArrayList<Patient> patients = new ArrayList<>();

			Bundle patientList = client.search()
					.forResource(Patient.class)
					.returnBundle(Bundle.class)
					.count(10)
					.execute();

			do {

				for (Bundle.BundleEntryComponent entry : patientList.getEntry()) {
					System.out.println(entry.getFullUrl());
					Patient p = (Patient) entry.getResource();
					System.out.println(p.getNameFirstRep().getNameAsSingleString());
					patients.add(p);
				}

				if (patientList.getLink(Bundle.LINK_NEXT) != null)
					patientList = null; // client.loadPage().next(patientList).execute();
				else
					patientList = null;
			}
			while (patientList != null);

			patientPCollection = input.apply(Create.of(patients));

			return patientPCollection.apply(ParDo.of(new DoFn<Patient, KV<String, Bundle>>() {
				@ProcessElement
				public void processElement(@Element Patient patient, OutputReceiver<KV<String, Bundle>> outputReceiver)
						throws IllegalAccessException, InstantiationException, NoSuchMethodException,
						InvocationTargetException {
					for (Class resource:RESOURCES) {
						Bundle b = new Bundle();

						b.addEntry().setResource((Resource) resource.getDeclaredConstructor().newInstance());
						outputReceiver.output(KV.of(patient.getId(), b));
					}

					Bundle patientBundle = new Bundle();
					patientBundle.addEntry().setResource(patient);

					outputReceiver.output(KV.of(patient.getId(), patientBundle));
				}
			}));
		}
	}

	// 2. Fetch Patient Resources
	public static class FetchPatientResources extends PTransform<PCollection<KV<String, Bundle>>, PCollection<KV<String, Bundle>>> {
		@Override
		public PCollection<KV<String, Bundle>> expand(PCollection<KV<String, Bundle>> resourceList) {
			return resourceList.apply(ParDo.of(new FetchResources()));
		}

	}

	// 3. Send Bundle of resources to server
	public static class SendPatientData
			extends PTransform<PCollection<KV<String, Bundle>>, PDone> {

		@Override
		public PDone expand(PCollection<KV<String, Bundle>> patientData) {

			patientData
					.apply(GroupByKey.create())
					.apply(ParDo.of(new DoFn<KV<String, Iterable<Bundle>>, Void>() {
						@ProcessElement
						public void processElement(@Element KV<String, Iterable<Bundle>> resources) {
							for (Bundle resource : resources.getValue()) {
								System.out.println("----\n" + ctx.newJsonParser().setPrettyPrint(false)
										.encodeResourceToString(resource));
							}
						}
					}));

			
			return PDone.in(patientData.getPipeline());
		}
	}

	// Helper DoFns
	static class FetchResources extends DoFn<KV<String, Bundle>, KV<String, Bundle>> {
		@ProcessElement
		public void processElement(@Element KV<String, Bundle> resourceInfo, OutputReceiver<KV<String,Bundle>> outputReceiver) {
			IGenericClient client = getSourceClient();
			Bundle result;
			String pId = resourceInfo.getKey();
			Bundle resourceBundle = resourceInfo.getValue();
			Resource resource = resourceBundle.getEntryFirstRep().getResource();

			if(resource instanceof Observation) result = client.search().forResource(Observation.class).where(Observation.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			else if(resource instanceof Condition) result = client.search().forResource(Condition.class).where(Condition.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			else if(resource instanceof Encounter) result = client.search().forResource(Encounter.class).where(Encounter.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			else if(resource instanceof ServiceRequest) result = client.search().forResource(ServiceRequest.class).where(ServiceRequest.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			else if(resource instanceof DiagnosticReport) result = client.search().forResource(DiagnosticReport.class).where(DiagnosticReport.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			else if(resource instanceof AllergyIntolerance) result = client.search().forResource(AllergyIntolerance.class).where(AllergyIntolerance.PATIENT.hasId(pId)).returnBundle(Bundle.class).execute();
			else result = resourceBundle;

			outputReceiver.output(KV.of(pId, result));
		}
	}

	// Helpers
	private static IGenericClient getSourceClient() {
		IClientInterceptor authInterceptor = new BasicAuthInterceptor("admin", "Admin123");
		ctx.getRestfulClientFactory().setSocketTimeout(200 * 1000);
		IGenericClient client = ctx.newRestfulGenericClient(SOURCE_URL);
		client.registerInterceptor(authInterceptor);
		return client;
	}

	// Pipeline Options
	public interface LocalFhirSyncOptions extends PipelineOptions {

		@Description("Path to the source endpoint")
		@Default.String("http://openmrs-server:8080/openmrs/ws/fhir2/R4")
		String getFhirSource();

		void setFhirSource(String url);

		@Description("Path to the destination endpoint")
		@Default.String("http://openmrs-hapi-server:8080/hapi-fhir-jpaserver/fhir")
		@Required
		String getFhirDest();

		void setFhirDest(String url);
	}

	// Pipeline Runner
	static void runLocalSync(LocalFhirSyncOptions options) {
		Collection<Patient> patientList;

		// Create the pipeline.
		Pipeline p = Pipeline.create(options);

		// Apply Create, passing the list and the coder, to create the PCollection.
		p.apply("Get Patients", new ReadPatients())
				.apply("FetchPatientResources", new FetchPatientResources())
				.apply("SendPatientData", new SendPatientData());
		p.run().waitUntilFinish();
	}

	// Main Fn
	public static void main(String[] args) {
		LocalFhirSyncOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(LocalFhirSyncOptions.class);

		SOURCE_URL = options.getFhirSource();
		DEST_URL = options.getFhirDest();

		runLocalSync(options);
	}
}

