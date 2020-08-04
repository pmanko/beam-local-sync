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
import java.util.Collection;
import java.util.Collections;

import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
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
import ca.uhn.fhir.rest.client.api.IGenericClient;

import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Patient;
import org.itech.shr.beam.httpio.HttpWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * *
 *
 * <p>To execute this pipeline, specify a local output file (if using the {@code DirectRunner}) or
 * output prefix on a supported distributed file system.
 *
 * <pre>{@code
 * --output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>The input file defaults to a public data set containing the text of of King Lear, by William
 * ShakespearWordCounte. You can override it and choose your own input with {@code --inputFile}.
 */
public class LocalFhirSync {

  // Create a context
  private static FhirContext ctx = FhirContext.forR4();

  /**
   * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
   * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
   * a ParDo in the pipeline.
   */
  static class CompilePatientBundle extends DoFn<Patient, Bundle> {

    private final String DEST_URL;

    CompilePatientBundle(String destUrl) {
      DEST_URL = destUrl;
    }

    @ProcessElement
    public void processElement(@Element Patient element, OutputReceiver<Bundle> receiver) {
      String pId = element.getId();

      Bundle patientBundle = new Bundle();
      patientBundle.setType(Bundle.BundleType.TRANSACTION);

      patientBundle.addEntry()
              .setResource(element.copy())
              .getRequest()
              .setUrl("Patient")
              .setMethod(Bundle.HTTPVerb.POST);

      IGenericClient client = ctx.newRestfulGenericClient(DEST_URL);
      Bundle resp = client.transaction().withBundle(patientBundle).execute();

      // Log the response
      System.out.println("Saved Patient Bundle:\n" + ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(resp));

    }
  }

  static class FhirReader extends PTransform<PBegin, PCollection<Patient>> {
    private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);

    private final String SOURCE_URL;


    public FhirReader(String fhirSource) {
      SOURCE_URL = fhirSource;
    }



    @Override
    public PCollection<Patient> expand(PBegin input) {
      // Create a client
      IClientInterceptor authInterceptor = new BasicAuthInterceptor("admin", "Admin123");
      ctx.getRestfulClientFactory().setSocketTimeout(200 * 1000);
      IGenericClient client = ctx.newRestfulGenericClient(SOURCE_URL);
      client.registerInterceptor(authInterceptor);

      ArrayList<Patient> patients = new ArrayList<>();

      Bundle patientList = client.search()
              .forResource(Patient.class)
              .returnBundle(Bundle.class)
              .count(100)
              .execute();

      do {

        for (Bundle.BundleEntryComponent entry: patientList.getEntry()) {
          System.out.println(entry.getFullUrl());
          Patient p = (Patient) entry.getResource();
          System.out.println(p.getNameFirstRep().getNameAsSingleString());
          patients.add(p);
        }

        if (patientList.getLink(Bundle.LINK_NEXT) != null)
          patientList = client.loadPage().next(patientList).execute();
        else
          patientList = null;
      }
      while (patientList != null);


      return input.apply(Create.of(patients));
    }
  }

  public static class SendPatientData
          extends PTransform<PCollection<Patient>, PCollection<Bundle>> {

    private final String DEST_URL;

    public SendPatientData(String fhirDest) {
      DEST_URL = fhirDest;
    }

    @Override
    public PCollection<Bundle> expand(PCollection<Patient> patients) {

      PCollection<Bundle> patientData = patients.apply(ParDo.of(new CompilePatientBundle(DEST_URL)));

      return patientData;
    }
  }

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

  static void runLocalSync(LocalFhirSyncOptions options) {
    Collection<Patient> patientList;

    // Create the pipeline.
    Pipeline p = Pipeline.create(options);

    // Apply Create, passing the list and the coder, to create the PCollection.
    p.apply("Get Patients", new FhirReader(options.getFhirSource())) // .setCoder(StringUtf8Coder.of());
      .apply("SendPatientData", new SendPatientData(options.getFhirDest()));
//      .apply("SendPatientBundle", new HttpWriter<>(ctx));

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    LocalFhirSyncOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(LocalFhirSyncOptions.class);

    runLocalSync(options);
  }
}
