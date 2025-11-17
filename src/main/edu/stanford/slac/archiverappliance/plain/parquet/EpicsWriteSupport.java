package edu.stanford.slac.archiverappliance.plain.parquet;

import static edu.stanford.slac.archiverappliance.plain.parquet.ParquetInfo.MetaDataKey;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.util.HashMap;
import java.util.Map;

/**
 * A Parquet write support for EPICS Archiver Appliance data.
 *
 * @param <T>
 */
public class EpicsWriteSupport<T extends Message> extends WriteSupport<T> {

    String pvName;
    short year;
    ArchDBRTypes archDBRTypes;
    ProtoWriteSupport<T> protoWriteSupport;

    /**
     * Creates a new write support.
     *
     * @param messageClass The message class to write.
     * @param pvName       The PV name to write to the footer.
     * @param year         The year to write to the footer.
     * @param archDBRTypes The ArchDBRType to write to the footer must correspond with the message class.
     */
    EpicsWriteSupport(Class<? extends Message> messageClass, String pvName, short year, ArchDBRTypes archDBRTypes) {
        this.pvName = pvName;
        this.year = year;
        this.archDBRTypes = archDBRTypes;
        this.protoWriteSupport = new ProtoWriteSupport<T>(messageClass);
    }

    /**
     * Initializes the write support.
     *
     * @param configuration the job's configuration
     * @return the write context
     */
    @Override
    public WriteContext init(Configuration configuration) {
        WriteContext writeContext = this.protoWriteSupport.init(configuration);

        Map<String, String> extraMetaData = new HashMap<>(writeContext.getExtraMetaData());
        extraMetaData.put(MetaDataKey.PV_NAME.key, this.pvName);
        extraMetaData.put(MetaDataKey.YEAR.key, String.valueOf(this.year));
        extraMetaData.put(MetaDataKey.TYPE.key, String.valueOf(this.archDBRTypes));
        return new WriteContext(writeContext.getSchema(), extraMetaData);
    }

    /**
     * Closes the write support.
     *
     * @param recordConsumer the recordConsumer to write to
     */
    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.protoWriteSupport.prepareForWrite(recordConsumer);
    }

    /**
     * Writes a record to the record consumer.
     *
     * @param record one record to write to the previously provided record consumer
     */
    @Override
    public void write(T record) {
        this.protoWriteSupport.write(record);
    }
}
