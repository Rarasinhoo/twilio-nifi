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
package com.fourmdg.processors.twiliowpp;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;


import com.twilio.Twilio;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;

@Tags({"twilio", "whatsapp"})
@CapabilityDescription("Envia mensagem WhatsApp para a API do Twilio")
@SeeAlso({})
@ReadsAttributes({
  @ReadsAttribute(attribute="wpp.para", description="Número que será enviada a mensagem."),
  @ReadsAttribute(attribute="wpp.corpo", description="Mensagem a ser enviada.")
})
@WritesAttributes({
  @WritesAttribute(attribute="wpp.sid", description="Código de identificação único da mensagem"),
	@WritesAttribute(attribute="wpp.price", description="Quanto custou enviar essa mensagem")
})
public class TwilioWpp extends AbstractProcessor {

    public static final PropertyDescriptor ACCOUNT_SID = new PropertyDescriptor.Builder()
            .name("ACCOUNT_SID")
            .displayName("SID da conta")
            .description("SID da conta Twilio")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_TOKEN = new PropertyDescriptor.Builder()
            .name("AUTH_TOKEN")
            .displayName("Token de autenticação")
            .description("Token de autenticação da conta Twilio")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FROM_NUMBER = new PropertyDescriptor.Builder()
            .name("FROM_NUMBER")
            .displayName("Remetente")
            .description("Número do remetente")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("sucesso")
            .description("Mensagem enviada com sucesso")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("falha")
            .description("Falha ao enviar mensagem")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ACCOUNT_SID);
        descriptors.add(AUTH_TOKEN);
        descriptors.add(FROM_NUMBER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        String Account_SID = context.getProperty(ACCOUNT_SID).getValue();
        String Auth_Token = context.getProperty(AUTH_TOKEN).getValue();
        String from = context.getProperty(FROM_NUMBER).getValue();
        String to = flowFile.getAttribute("wpp.para");
        String msg = flowFile.getAttribute("wpp.corpo");
    
        
    
        try {

          Twilio.init(Account_SID, Auth_Token);
          Message message = Message.creator(
                  new com.twilio.type.PhoneNumber(to),
                  new com.twilio.type.PhoneNumber(from),
                  msg)
              .create();

          Map<String, String> attributes = new HashMap<String, String>();
			    attributes.put("wpp.sid", message.getSid());
			    attributes.put("wpp.preco", message.getPrice());
          
          flowFile = session.putAllAttributes(flowFile, attributes);
          session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
          flowFile = session.penalize(flowFile);
          session.transfer(flowFile, REL_FAILURE);
        }
    }
}
