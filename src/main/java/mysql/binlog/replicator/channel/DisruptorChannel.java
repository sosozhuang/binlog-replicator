package mysql.binlog.replicator.channel;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.EventHandlerGroup;
import com.lmax.disruptor.dsl.ProducerType;
import mysql.binlog.replicator.concurrent.SimpleThreadFactory;
import mysql.binlog.replicator.metric.EventMetricCollector;
import mysql.binlog.replicator.metric.MetricStore;
import mysql.binlog.replicator.model.ReplicatorMessage;
import mysql.binlog.replicator.sink.KafkaProperties;
import mysql.binlog.replicator.sink.KafkaSink;
import mysql.binlog.replicator.sink.Sink;
import mysql.binlog.replicator.source.Source;
import mysql.binlog.replicator.util.AbstractLifeCycle;
import mysql.binlog.replicator.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class DisruptorChannel extends AbstractLifeCycle implements Channel<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorChannel.class);
    private final Disruptor<ReplicatorMessage> disruptor;
    private final MessageTranslator translator;
    private final List<Sink<ReplicatorMessage>> sinks;

    public DisruptorChannel(Configuration config,
                            ClientIdentity clientId,
                            Source source,
                            MetricStore metricStore,
                            EventMetricCollector collector) {
        DisruptorProperties properties = new DisruptorProperties(config.getSubConfig("replicator.disruptor"));
        this.disruptor = new Disruptor<>(new MessageEventFactory(), properties.getEntryBufferSize(),
                new SimpleThreadFactory("disruptor-" + clientId.getDestination()),
                ProducerType.SINGLE, properties.getWaitStrategy());
        this.translator = new MessageTranslator();

        KafkaProperties kafkaProperties = new KafkaProperties(config.getSubConfig("replicator.kafka"));
        KafkaSink[] sinks = IntStream.range(0, properties.getEntryWorkerThreads())
                .mapToObj(i -> new KafkaSink(kafkaProperties, clientId, metricStore))
                .toArray(KafkaSink[]::new);
        collector.addMetricHolders(clientId.getDestination(), sinks);
        this.sinks = Arrays.asList(sinks);

        MessageHandler[] messageHandlers = this.sinks.stream()
                .map(sink -> new MessageHandler(clientId, sink))
                .toArray(MessageHandler[]::new);
        collector.addMetricHolders(clientId.getDestination(), messageHandlers);
        EventHandlerGroup<ReplicatorMessage> handlerGroup = this.disruptor.handleEventsWithWorkerPool(messageHandlers);

        handlerGroup.then(new FutureHandler(source));
        this.disruptor.setDefaultExceptionHandler(new LoggerExceptionHandler<>());
    }

    @Override
    protected void doStart() {
        disruptor.start();
        for (Sink<?> sink : sinks) {
            sink.start();
        }
    }

    @Override
    protected void doStop() {
        try {
            disruptor.shutdown(30, TimeUnit.SECONDS);
        } catch (Throwable e) {
            LOGGER.error("Failed to stop disruptor.", e);
        }
        for (Sink<?> sink : sinks) {
            sink.stop();
        }
    }

    @Override
    public void publish(Message message) {
        disruptor.publishEvent(translator, message);
    }
}
