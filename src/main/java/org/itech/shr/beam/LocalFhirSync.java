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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.hl7.fhir.r4.model.AllergyIntolerance;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
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
	private static List<Class> RESOURCES = Arrays.asList(Encounter.class, Condition.class, DiagnosticReport.class, /*ServiceRequest.class,*/ AllergyIntolerance.class);


	// Bundle resp = client.transaction().withBundle(patientBundle).execute();

	// Transforms

	// 1. Read in patient list
	static class ReadPatients extends PTransform<PBegin, PCollection<Bundle>> {

		private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);

		@Override
		public PCollection<Bundle> expand(PBegin input) {
			// Create a client
			IGenericClient client = getSourceClient();

			PCollection<Patient> patientPCollection;
			PCollection<Class> resourcePCollection;
			PCollection<Bundle> patientResources;

			ArrayList<Bundle> patientBundles = new ArrayList<>();

			Bundle patientList = client.search()
					.forResource(Patient.class)
					.returnBundle(Bundle.class)
					.count(10)
					.execute();

			do {

				for (Bundle.BundleEntryComponent entry : patientList.getEntry()) {
					System.out.println(entry.getFullUrl());
					Patient p = (Patient) entry.getResource();
					Bundle pBundle = new Bundle();
					pBundle.setType(Bundle.BundleType.TRANSACTION)
							.addEntry()
							.setFullUrl(p.getId())
							.setResource(p)
							.getRequest()
							.setUrl("Patient")
							.setMethod(Bundle.HTTPVerb.POST);

					System.out.println(p.getId());
					patientBundles.add(pBundle);
				}

				if (patientList.getLink(Bundle.LINK_NEXT) != null)
					patientList = null; // client.loadPage().next(patientList).execute();
				else
					patientList = null;
			}
			while (patientList != null);

			patientResources = input.apply(Create.of(patientBundles));

			return patientResources;
		}
	}

	// 2. Fetch Patient Resources
	public static class FetchPatientResources extends PTransform<PCollection<Bundle>, PCollection<Bundle>> {
		@Override
		public PCollection<Bundle> expand(PCollection<Bundle> resourceList) {
			return resourceList.apply(ParDo.of(new FetchResources()));
		}

	}

	// 3. Send Bundle of resources to server
	public static class SendPatientData
			extends PTransform<PCollection<Bundle>, PDone> {

		@Override
		public PDone expand(PCollection<Bundle> patientData) {

			patientData.apply(ParDo.of(new DoFn<Bundle, Void>() {
				@ProcessElement
				public void processElement(@Element Bundle resources) {
					System.out.println("Posting bundle of " + resources.getEntry().size() + " for Patient " + resources.getEntryFirstRep().getId());
					IGenericClient client = ctx.newRestfulGenericClient(DEST_URL);
					Bundle resp = client.transaction().withBundle(resources.copy()).execute();

					// Log the response
					System.out.println("Saved Patient Bundle:\n" + ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(resp));

				}
			}));

			return PDone.in(patientData.getPipeline());
		}
	}

	// Helper DoFns
	static class FetchResources extends DoFn<Bundle, Bundle> {
		@ProcessElement
		public void processElement(@Element Bundle patientBundle, OutputReceiver<Bundle> outputReceiver) {
			IGenericClient client = getSourceClient();
			Bundle result = patientBundle.copy();
			Bundle resourceList;

//			Bundle resourceBundle = resourceInfo.getValue();
			Patient p = (Patient) result.getEntryFirstRep().getResource();
			String pId = p.getId().split("/")[7];

			// result = client.search().forResource(Condition.class).where(Condition.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();

			// TODO: PARALLELIZE
			resourceList = client.search().forResource(Observation.class).where(Observation.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			addToBundle(result, resourceList);

			resourceList = client.search().forResource(Condition.class).where(Condition.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			addToBundle(result, resourceList);

			resourceList = client.search().forResource(Encounter.class).where(Encounter.SUBJECT.hasId(pId)).returnBundle(Bundle.class).execute();
			addToBundle(result, resourceList);

			resourceList = client.search().forResource(AllergyIntolerance.class).where(AllergyIntolerance.PATIENT.hasId(pId)).returnBundle(Bundle.class).execute();
			addToBundle(result, resourceList);

			outputReceiver.output(result);
		}

		private void addToBundle(Bundle result, Bundle resourceList) {
			for (Bundle.BundleEntryComponent entry : resourceList.getEntry()) {
				System.out.println(entry.getResource().getId());
				result.addEntry()
						.setFullUrl(entry.getId())
						.setResource(entry.getResource())
						.getRequest()
						.setUrl(entry.fhirType())
						.setMethod(Bundle.HTTPVerb.POST);
			}
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
				//.apply(GroupByKey.create())
//				.apply(Keys.<String>create())
//				.apply("WriteNames", TextIO.write().to("/dev/beam/hmm.txt"));

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

