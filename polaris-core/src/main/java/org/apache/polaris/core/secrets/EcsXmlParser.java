/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.secrets;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/** Small utility to parse ECS/STs AssumeRole XML responses and extract credentials. */
public final class EcsXmlParser {

  private EcsXmlParser() {}

  public static Credentials parse(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

    NodeList credentialsList = doc.getElementsByTagNameNS("*", "Credentials");
    if (credentialsList.getLength() == 0) {
      credentialsList = doc.getElementsByTagName("Credentials");
    }

    if (credentialsList.getLength() == 0) {
      return null;
    }

    Element credentialsElement = (Element) credentialsList.item(0);

    String accessKeyId = getElementTextContent(credentialsElement, "AccessKeyId");
    String secretAccessKey = getElementTextContent(credentialsElement, "SecretAccessKey");
    String sessionToken = getElementTextContent(credentialsElement, "SessionToken");
    String expiration = getElementTextContent(credentialsElement, "Expiration");

    return new Credentials(accessKeyId, secretAccessKey, sessionToken, expiration);
  }

  private static String getElementTextContent(Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagNameNS("*", tagName);
    if (nodes.getLength() == 0) {
      nodes = parent.getElementsByTagName(tagName);
    }

    if (nodes.getLength() > 0) {
      return nodes.item(0).getTextContent();
    }
    return null;
  }

  public static class Credentials {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String expiration;

    public Credentials(
        String accessKeyId, String secretAccessKey, String sessionToken, String expiration) {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
      this.sessionToken = sessionToken;
      this.expiration = expiration;
    }

    public String getAccessKeyId() {
      return accessKeyId;
    }

    public String getSecretAccessKey() {
      return secretAccessKey;
    }

    public String getSessionToken() {
      return sessionToken;
    }

    public String getExpiration() {
      return expiration;
    }
  }
}
