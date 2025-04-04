package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MetadataRepoBasedTopicExistingCheckerImpl implements TopicExistenceChecker {
  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private static final Logger LOGGER = LogManager.getLogger(MetadataRepoBasedTopicExistingCheckerImpl.class);

  public MetadataRepoBasedTopicExistingCheckerImpl(ReadOnlyStoreRepository readOnlyStoreRepository) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
  }

  public boolean checkTopicExists(String topic) {
    boolean isExistingTopic = true;

    try {
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      Store store = readOnlyStoreRepository.getStoreOrThrow(storeName);

      if (Version.isVersionTopicOrStreamReprocessingTopic(topic)) {
        int version = Version.parseVersionFromKafkaTopicName(topic);
        if (store.getVersion(version) == null) {
          LOGGER.warn("Version {} not found for topic: {}", version, topic);
          isExistingTopic = false;
        }
      } else if (Version.isRealTimeTopic(topic)) {
        if (!store.isHybrid()) {
          LOGGER.warn("Store {} is not hybrid currently, but found real-time topic {}", storeName, topic);
          isExistingTopic = false;
        }
      }
    } catch (VeniceNoStoreException e) {
      LOGGER.warn("Store not found for topic: {}", topic);
      isExistingTopic = false;
    } catch (Exception e) {
      LOGGER.error("Exception thrown in checkTopicExists: ", e);
    }
    return isExistingTopic;
  }
}
