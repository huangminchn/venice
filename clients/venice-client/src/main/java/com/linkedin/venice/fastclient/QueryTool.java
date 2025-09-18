package com.linkedin.venice.fastclient;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.fastclient.transport.HttpClient5BasedR2Client;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.tehuti.metrics.MetricsRepository;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A tool use thin client to query the value from a store by the specified key.
 * This version supports both router-based requests (thin client) and server-direct requests (fast client).
 */
public class QueryTool {
  private static final Logger LOGGER = LogManager.getLogger(QueryTool.class);
  private static final int STORE = 0;
  private static final int KEY_STRING = 1;
  private static final int URL = 2;
  private static final int IS_VSON_STORE = 3;
  private static final int SSL_CONFIG_FILE_PATH = 4;
  private static final int IS_SERVER_DIRECT = 5;
  private static final int REQUIRED_ARGS_COUNT = 6;

  public static void main(String[] args) throws Exception {
    if (args.length < REQUIRED_ARGS_COUNT) {
      System.out.println(
          "Usage: java -jar venice-client-0.1.jar <store> <key_string> <url> <is_vson_store> <ssl_config_file_path> <is_server_direct>");
      System.exit(1);
    }
    String store = removeQuotes(args[STORE]);
    String keyString = removeQuotes(args[KEY_STRING]);
    String url = removeQuotes(args[URL]);
    boolean isVsonStore = Boolean.parseBoolean(removeQuotes(args[IS_VSON_STORE]));
    String sslConfigFilePath = removeQuotes(args[SSL_CONFIG_FILE_PATH]);
    boolean isServerDirect = Boolean.parseBoolean(removeQuotes(args[IS_SERVER_DIRECT]));
    Optional<String> sslConfigFilePathArgs =
        StringUtils.isEmpty(sslConfigFilePath) ? Optional.empty() : Optional.of(sslConfigFilePath);
    System.out.println();

    Map<String, String> outputMap =
        queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFilePathArgs, isServerDirect);
    outputMap.entrySet().stream().forEach(System.out::println);
  }

  public static Map<String, String> queryStoreForKey(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile) throws Exception {
    return queryStoreForKey(store, keyString, url, isVsonStore, sslConfigFile, false);
  }

  public static Map<String, String> queryStoreForKey(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      Optional<String> sslConfigFile,
      boolean isServerDirect) throws Exception {

    SSLFactory factory = null;
    if (sslConfigFile.isPresent()) {
      Properties sslProperties = SslUtils.loadSSLConfig(sslConfigFile.get());
      String sslFactoryClassName = sslProperties.getProperty(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      factory = SslUtils.getSSLFactory(sslProperties, sslFactoryClassName);
    }

    // Verify the ssl engine is set up correctly.
    if (url.toLowerCase().trim().startsWith("https") && (factory == null || factory.getSSLContext() == null)) {
      throw new VeniceException("ERROR: The SSL configuration is not valid to send a request to " + url);
    }

    Map<String, String> outputMap = new LinkedHashMap<>();

    if (isServerDirect) {
      // Use fast client for server-direct requests
      return queryWithFastClient(store, keyString, url, isVsonStore, factory, outputMap);
    } else {
      // // Use thin client for router requests
      // return queryWithThinClient(store, keyString, url, isVsonStore, factory, outputMap);
      // Not supported yet.
      throw new VeniceUnsupportedOperationException("Router-based request is not supported yet.");
    }
  }

  private static Map<String, String> queryWithFastClient(
      String store,
      String keyString,
      String url,
      boolean isVsonStore,
      SSLFactory sslFactory,
      Map<String, String> outputMap) throws Exception {

    D2Client d2Client = getAndStartD2Client("zk-ltx1-d2.stg.linkedin.com:12913", true, sslFactory);
    LOGGER.info("[DEBUGDEBUG] d2Client started: {}", d2Client != null);
    try {
      Client r2Client = HttpClient5BasedR2Client.getR2Client(sslFactory.getSSLContext(), 8, 5000);
      LOGGER.info("[DEBUGDEBUG] r2Client started: {}", r2Client != null);
      ClientConfig.ClientConfigBuilder clientConfigBuilder =
          new ClientConfig.ClientConfigBuilder<>().setStoreName(store)
              .setR2Client(r2Client)
              .setSpeculativeQueryEnabled(true)
              .setDualReadEnabled(false);
      clientConfigBuilder.setStoreMetadataFetchMode(SERVER_BASED_METADATA);
      clientConfigBuilder.setD2Client(d2Client);
      clientConfigBuilder.setClusterDiscoveryD2Service("venice-discovery");
      clientConfigBuilder.setMetadataRefreshIntervalInSeconds(5);
      MetricsRepository metricsRepository = new MetricsRepository();
      clientConfigBuilder.setMetricsRepository(metricsRepository);

      // AvroGenericStoreClient<Object, Object> genericFastClient =
      // ClientFactory.getAndStartGenericStoreClient(clientConfigBuilder.build());

      try (AvroGenericStoreClient<Object, Object> client =
          ClientFactory.getAndStartGenericStoreClient(clientConfigBuilder.build())) {
        LOGGER.info("[DEBUGDEBUG] fast client started: {}", client != null);
        Schema keySchema = client.getKeySchema();

        // Transfer vson schema to avro schema.
        while (keySchema.getType().equals(Schema.Type.UNION)) {
          keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
        }

        if (keyString.startsWith("[") || keyString.startsWith("'[")) {
          // This is a list of keys.
          Set<Object> keys = convertKeys(keyString, keySchema);
          LOGGER.info("[DEBUGDEBUG] Start sending requests to server");
          Map<Object, Object> values = client.batchGet(keys).get(15, TimeUnit.SECONDS);
          LOGGER.info("[DEBUGDEBUG] Received responses from server");
          outputMap.put("key-class", keys.iterator().next().getClass().getCanonicalName());
          outputMap.put(
              "value-class",
              values.isEmpty() ? "null" : values.values().iterator().next().getClass().getCanonicalName());
          outputMap.put("request-type", "server-direct");
          outputMap.put("keys", keyString);
          outputMap.put("values", values.toString());
          return outputMap;
        } else {
          Object key = convertKey(keyString, keySchema);
          System.out.println("Key string parsed successfully. About to make the query.");

          Object value = client.get(key).get(15, TimeUnit.SECONDS);

          outputMap.put("key-class", key.getClass().getCanonicalName());
          outputMap.put("value-class", value == null ? "null" : value.getClass().getCanonicalName());
          outputMap.put("request-type", "server-direct");
          outputMap.put("key", keyString);
          outputMap.put("value", value == null ? "null" : value.toString());
          return outputMap;
        }
      }
    } finally {
      // d2Client.shutdown();
    }
  }
  //
  // private static Map<String, String> queryWithThinClient(
  // String store,
  // String keyString,
  // String url,
  // boolean isVsonStore,
  // SSLFactory factory,
  // Map<String, String> outputMap) throws Exception {
  //
  // try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(
  // ClientConfig.defaultGenericClientConfig(store)
  // .setVeniceURL(url)
  // .setVsonClient(isVsonStore)
  // .setSslFactory(factory))) {
  // AbstractAvroStoreClient<Object, Object> castClient =
  // (AbstractAvroStoreClient<Object, Object>) ((StatTrackingStoreClient<Object, Object>) client)
  // .getInnerStoreClient();
  // Schema keySchema = castClient.getKeySchema();
  //
  // // Transfer vson schema to avro schema.
  // while (keySchema.getType().equals(Schema.Type.UNION)) {
  // keySchema = VsonAvroSchemaAdapter.stripFromUnion(keySchema);
  // }
  // if (keyString.startsWith("[") || keyString.startsWith("'[")) {
  // // This is a list of keys.
  // Set<Object> keys = convertKeys(keyString, keySchema);
  // Map<Object, Object> values = client.batchGet(keys).get(15, TimeUnit.SECONDS);
  // outputMap.put("key-class", keys.iterator().next().getClass().getCanonicalName());
  // outputMap.put(
  // "value-class",
  // values.isEmpty() ? "null" : values.values().iterator().next().getClass().getCanonicalName());
  // outputMap.put("request-payload", castClient.getRequestPayloadByKeys(keys));
  // outputMap.put("byte-to-integer string", castClient.getByteToIntegerString(keys));
  // outputMap.put("request-type", "router");
  // outputMap.put("keys", keyString);
  // outputMap.put("values", values.toString());
  // return outputMap;
  // } else {
  // Object key = null;
  // key = convertKey(keyString, keySchema);
  // System.out.println("Key string parsed successfully. About to make the query.");
  //
  // Object value = client.get(key).get(15, TimeUnit.SECONDS);
  //
  // outputMap.put("key-class", key.getClass().getCanonicalName());
  // outputMap.put("value-class", value == null ? "null" : value.getClass().getCanonicalName());
  // outputMap.put("request-path", castClient.getRequestPathByKey(key));
  // outputMap.put("request-type", "router");
  // outputMap.put("key", keyString);
  // outputMap.put("value", value == null ? "null" : value.toString());
  // return outputMap;
  // }
  // }
  // }

  public static Set<Object> convertKeys(String keyString, Schema keySchema) {
    // The key string will be like '[{"uniqueID": 247589500, "dummyStr":"+-~V::~AWY
    // smA=tL0JD~x2,Yuv&B257G/mp7:m+ED(T;aTb.O\\"},{"uniqueID": 2222, "dummyStr":"+-~V::~AWY
    // smA=tL0JD~x2,Yuv&B257G/mp7:m+ED(T;aTb.O\\"},{"uniqueID": 247589500, "dummyStr":"+-~V::~AWY
    // smA=tL0JD~x2,Yuv&B257G/mp7:m+ED(T;aTb.O\\"}]'
    // Break it down to a list of key strings.
    LOGGER.info("[DEBUGDEBUG] keyString: {}", keyString);
    String[] keyStrings = keyString.substring(1, keyString.length() - 1).split(",,,,,");
    Set<Object> keys = new LinkedHashSet<>();
    for (String keyStr: keyStrings) {
      keys.add(convertKey(keyStr, keySchema));
    }
    return keys;
  }

  public static Object convertKey(String keyString, Schema keySchema) {
    Object key;
    switch (keySchema.getType()) {
      case INT:
        key = Integer.parseInt(keyString);
        break;
      case LONG:
        key = Long.parseLong(keyString);
        break;
      case FLOAT:
        key = Float.parseFloat(keyString);
        break;
      case DOUBLE:
        key = Double.parseDouble(keyString);
        break;
      case BOOLEAN:
        key = Boolean.parseBoolean(keyString);
        break;
      case STRING:
        key = keyString;
        break;
      default:
        try {
          key = new GenericDatumReader<>(keySchema, keySchema).read(
              null,
              AvroCompatibilityHelper.newJsonDecoder(keySchema, new ByteArrayInputStream(keyString.getBytes())));
        } catch (IOException e) {
          throw new VeniceException("Invalid input key:" + keyString, e);
        }
        break;
    }
    return key;
  }

  public static String removeQuotes(String str) {
    String result = str;
    if (result.startsWith("\"")) {
      result = result.substring(1);
    }
    if (str.endsWith("\"")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  public static D2Client getD2Client(String zkHosts, boolean https, SSLFactory sslFactory) {
    return getD2Client(zkHosts, https, HttpProtocolVersion.HTTP_1_1, sslFactory);
  }

  public static D2Client getD2Client(
      String zkHosts,
      boolean https,
      HttpProtocolVersion httpProtocolVersion,
      SSLFactory sslFactory) {
    int sessionTimeout = 5000;
    String basePath = "/d2";

    if (httpProtocolVersion.equals(HttpProtocolVersion.HTTP_2) && !https) {
      throw new VeniceException("Param 'https' needs to be 'true' when enabling http/2");
    }
    Map<String, TransportClientFactory> transportClients = new HashMap<>();
    TransportClientFactory httpTransport =
        new HttpClientFactory.Builder().setUsePipelineV2(true).setDefaultHttpVersion(httpProtocolVersion).build();
    transportClients.put("http", httpTransport);
    transportClients.put("https", httpTransport);

    D2ClientBuilder builder = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setZkStartupTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setLbWaitTimeout(sessionTimeout, TimeUnit.MILLISECONDS)
        .setBasePath(basePath)
        .setClientFactories(transportClients);

    if (https) {
      // SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
      builder.setSSLContext(sslFactory.getSSLContext())
          .setSSLParameters(sslFactory.getSSLParameters())
          .setIsSSLEnabled(true);
    }

    return builder.build();
  }

  public static void startD2Client(D2Client d2Client) {
    CountDownLatch latch = new CountDownLatch(1);
    d2Client.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        throw new RuntimeException("d2client throws error on startup", e);
      }

      @Override
      public void onSuccess(None result) {
        latch.countDown();
      }
    });
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new VeniceException(e);
    }
  }

  public static D2Client getAndStartD2Client(String zkHosts, boolean https, SSLFactory sslFactory) {
    D2Client d2Client = getD2Client(zkHosts, https, sslFactory);
    startD2Client(d2Client);
    return d2Client;
  }
}
