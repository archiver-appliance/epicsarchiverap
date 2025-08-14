package edu.stanford.slac.archiverappliance.PB.data;

import com.google.protobuf.Message;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.util.HashMap;

/**
 * A static utility class that provides a mapping between EPICS {@link ArchDBRTypes}
 * and their corresponding Google Protobuf {@link Message} classes.
 * <p>
 * This class acts as a type-safe, bidirectional lookup table, essential for the
 * serialization and deserialization of EPICS events into the Parquet format. It ensures
 * that the correct Protobuf message type is used for each DBR type.
 *
 * @author skybrewer
 * @see ArchDBRTypes
 * @see EPICSEvent
 */
public class DBR2PBMessageTypeMapping {

    /**
     * The map holding the direct association from an ArchDBRType to its mapping object.
     * An EnumMap is used for high performance and type safety with enum keys.
     */
    private static final HashMap<ArchDBRTypes, DBR2PBMessageTypeMapping> typemap = new HashMap<>();

    /*
     * Static initializer block to populate the type map.
     * It exhaustively maps every supported DBR type to its corresponding Protobuf message class.
     * A final check ensures that all enum values in {@link ArchDBRTypes} have been mapped,
     * preventing runtime errors if new types are added without updating this mapping.
     */
    static {
        typemap.put(ArchDBRTypes.DBR_SCALAR_STRING, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarString.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_SHORT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarShort.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_FLOAT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarFloat.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_ENUM, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarEnum.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_BYTE, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarByte.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_INT, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarInt.class));
        typemap.put(ArchDBRTypes.DBR_SCALAR_DOUBLE, new DBR2PBMessageTypeMapping(EPICSEvent.ScalarDouble.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_STRING, new DBR2PBMessageTypeMapping(EPICSEvent.VectorString.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_SHORT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorShort.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_FLOAT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorFloat.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_ENUM, new DBR2PBMessageTypeMapping(EPICSEvent.VectorEnum.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_BYTE, new DBR2PBMessageTypeMapping(EPICSEvent.VectorChar.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_INT, new DBR2PBMessageTypeMapping(EPICSEvent.VectorInt.class));
        typemap.put(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, new DBR2PBMessageTypeMapping(EPICSEvent.VectorDouble.class));
        typemap.put(ArchDBRTypes.DBR_V4_GENERIC_BYTES, new DBR2PBMessageTypeMapping(EPICSEvent.V4GenericBytes.class));

        for (ArchDBRTypes t : ArchDBRTypes.values()) {
            if (typemap.get(t) == null) {
                throw new RuntimeException("We have a type in DBR type that does have an equivalent PB type");
            }
        }
    }

    Class<? extends Message> pbclass;

    private DBR2PBMessageTypeMapping(Class<? extends Message> pblass) {
        this.pbclass = pblass;
    }

    /**
     * Retrieves the Protobuf {@link Message} class that corresponds to the given {@link ArchDBRTypes}.
     *
     * @param type The EPICS DBR type to look up.
     * @return The corresponding Protobuf message {@code Class}.
     * @throws NullPointerException if the provided type has no mapping (which is prevented by the static initializer).
     */
    public static Class<? extends Message> getMessageClass(ArchDBRTypes type) {
        return typemap.get(type).pbclass;
    }
}
