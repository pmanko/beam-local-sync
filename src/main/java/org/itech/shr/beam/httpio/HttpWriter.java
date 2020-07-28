package org.itech.shr.beam.httpio;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.MethodOutcome;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A DoFn to write to Http.
 */
public class HttpWriter<T> extends PTransform<PCollection<Bundle>, PDone> {
    private static final Logger logger = LoggerFactory.getLogger(HttpWriter.class);
    private static FhirContext fhirContext = FhirContext.forR4();

    @Override
    public PDone expand(PCollection<Bundle> input) {
        input.apply(ParDo.of(new Fn<>()));
        return PDone.in(input.getPipeline());
    }

    private static class Fn<T> extends DoFn<Bundle, String> {

        private IGenericClient fhirClient;

        private Fn() {
        }

        @Setup
        public void onSetup(ProcessContext context) {
            this.fhirClient = fhirContext.newRestfulGenericClient("localhost:8082");
        }

        @ProcessElement
        public void onElement(@Element Bundle element, ProcessContext context) {
            //logger.info("The input in writer: "+element);
            // Invoke the server create method (and send pretty-printed JSON
            // encoding to the server
            // instead of the default which is non-pretty printed XML)
            MethodOutcome outcome = this.fhirClient.create()
                    .resource(element)
                    .prettyPrint()
                    .encodedJson()
                    .execute();

            // The MethodOutcome object will contain information about the
            // response from the server, including the ID of the created
            // resource, the OperationOutcome response, etc. (assuming that
            // any of these things were provided by the server! They may not
            // always be)
            IIdType id = outcome.getId();
            System.out.println("Got ID: " + id.getValue());        }
    }
}

