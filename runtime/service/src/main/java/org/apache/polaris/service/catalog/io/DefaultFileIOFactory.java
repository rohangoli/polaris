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
package org.apache.polaris.service.catalog.io;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.AccessConfig;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}. For example, it evaluates storage actions and retrieves subscoped
 * credentials to initialize a {@link FileIO} instance with the most limited permissions necessary.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final StorageCredentialCache storageCredentialCache;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileIOFactory.class);

  @Inject
  public DefaultFileIOFactory(
      StorageCredentialCache storageCredentialCache,
      MetaStoreManagerFactory metaStoreManagerFactory) {
    this.storageCredentialCache = storageCredentialCache;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull CallContext callContext,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
    RealmContext realmContext = callContext.getRealmContext();
    PolarisCredentialVendor credentialVendor =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext);

    // Get subcoped creds
    properties = new HashMap<>(properties);
    // Polaris property keys for endpoint and path-style. Declare early so debug
    // instrumentation can examine AccessConfig contents before we merge values.
    String polarisEndpointKey =
        org.apache.polaris.core.storage.StorageAccessProperty.AWS_ENDPOINT.getPropertyName();
    String polarisPathStyleKey =
        org.apache.polaris.core.storage.StorageAccessProperty.AWS_PATH_STYLE_ACCESS
            .getPropertyName();
    Optional<PolarisEntity> storageInfoEntity =
        FileIOUtil.findStorageInfoFromHierarchy(resolvedEntityPath);
    // Log storage info entity internal properties at TRACE level (no stdout prints).
    if (storageInfoEntity.isPresent() && LOG.isTraceEnabled()) {
      try {
        PolarisEntity si = storageInfoEntity.get();
        LOG.trace(
            "Found storageInfoEntity id={} internalProperties={}",
            si.getId(),
            si.getInternalPropertiesAsMap());
      } catch (Exception e) {
        LOG.trace("Failed to stringify storageInfoEntity for trace logging", e);
      }
    }
    Optional<AccessConfig> accessConfig =
        storageInfoEntity.map(
            storageInfo ->
                FileIOUtil.refreshAccessConfig(
                    callContext,
                    storageCredentialCache,
                    credentialVendor,
                    identifier,
                    tableLocations,
                    storageActions,
                    storageInfo,
                    Optional.empty()));

    // Trace-level log for AccessConfig diagnostics; remove stdout diagnostics.
    if (accessConfig.isPresent() && LOG.isTraceEnabled()) {
      try {
        AccessConfig ac = accessConfig.get();
        String polarisEndpointValCred = ac.credentials().get(polarisEndpointKey);
        String polarisEndpointValExtra = ac.extraProperties().get(polarisEndpointKey);
        String polarisEndpointValInternal = ac.internalProperties().get(polarisEndpointKey);
        String polarisPathValCred = ac.credentials().get(polarisPathStyleKey);
        String polarisPathValExtra = ac.extraProperties().get(polarisPathStyleKey);
        String polarisPathValInternal = ac.internalProperties().get(polarisPathStyleKey);
        LOG.trace(
            "AccessConfig detected: credentialsKeys={}, extraKeys={}, internalKeys={} -> endpointCred='{}', endpointExtra='{}', endpointInternal='{}', pathCred='{}', pathExtra='{}', pathInternal='{}'",
            ac.credentials().keySet(),
            ac.extraProperties().keySet(),
            ac.internalProperties().keySet(),
            polarisEndpointValCred,
            polarisEndpointValExtra,
            polarisEndpointValInternal,
            polarisPathValCred,
            polarisPathValExtra,
            polarisPathValInternal);
      } catch (Exception e) {
        LOG.trace("Failed to stringify AccessConfig for trace logging", e);
      }
    }

    // Update the FileIO with the subscoped credentials
    // Update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    if (accessConfig.isPresent()) {
      properties.putAll(accessConfig.get().credentials());
      properties.putAll(accessConfig.get().extraProperties());
      properties.putAll(accessConfig.get().internalProperties());
    }

    // Some storage configurations use Polaris-specific property keys (e.g. "s3.endpoint",
    // "s3.path-style-access"). Ensure those values are also present under the Iceberg
    // S3FileIO/AWS client property keys so the Iceberg S3FileIO will configure the
    // underlying AWS SDK client with an endpoint override and path-style access.
    // This translation avoids coupling the core storage model to Iceberg constants elsewhere.
    if (properties.containsKey(polarisEndpointKey)) {
      // S3FileIO expects S3FileIOProperties.ENDPOINT ("s3.endpoint")
      properties.put(S3FileIOProperties.ENDPOINT, properties.get(polarisEndpointKey));
      // Note: we intentionally set the endpoint on S3FileIOProperties so the Iceberg S3FileIO
      // will configure the underlying client with the provided endpoint. Older/newer
      // Iceberg versions may not expose a generic AwsClientProperties.ENDPOINT constant,
      // so avoid referencing it directly to remain compatible with the project's Iceberg
      // dependency.
    }
    if (properties.containsKey(polarisPathStyleKey)) {
      properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, properties.get(polarisPathStyleKey));
    }

    // Trace-level diagnostics for FileIO init properties; avoid stdout prints.
    if (LOG.isTraceEnabled()) {
      try {
        String polarisEndpointVal = properties.get(polarisEndpointKey);
        String s3EndpointVal = properties.get(S3FileIOProperties.ENDPOINT);
        String polarisPathVal = properties.get(polarisPathStyleKey);
        String s3PathVal = properties.get(S3FileIOProperties.PATH_STYLE_ACCESS);
        boolean hasToken =
            properties.containsKey(StorageAccessProperty.AWS_TOKEN.getPropertyName());
        LOG.trace(
            "FileIO init properties: polaris_endpoint='{}', s3.endpoint='{}', polaris_pathStyle='{}', s3.pathStyle='{}', hasToken={}",
            polarisEndpointVal,
            s3EndpointVal,
            polarisPathVal,
            s3PathVal,
            hasToken);
      } catch (Exception e) {
        LOG.trace("Failed to stringify FileIO properties for trace logging", e);
      }
    }

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    return new ExceptionMappingFileIO(CatalogUtil.loadFileIO(ioImplClassName, properties, null));
  }
}
