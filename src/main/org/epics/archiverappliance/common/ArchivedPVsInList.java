package org.epics.archiverappliance.common;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;

import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicates;

/*
 * Use a Hz query to determine the PV's that are being archived in this cluster.
 */
public class ArchivedPVsInList {

    private static record OnlyFields ( String pvName, String[] archiveFields ) implements Serializable {};
    private static class FieldsProjection implements Projection<Map.Entry<String, PVTypeInfo>, OnlyFields> {
        @Override
        public OnlyFields transform(Map.Entry<String, PVTypeInfo> entry) {
            String pvName = entry.getKey();
            PVTypeInfo value = entry.getValue();
            return new OnlyFields(pvName, value.getArchiveFields()) ;
        }
    }

	public static List<String> getArchivedPVs(List<String> pvNames, ConfigService configService) throws IOException {
		record PVNameParts(String pvName, String plainPVName, boolean isField, String fieldName){};
		LinkedList<PVNameParts> pvnps = new LinkedList<>();
		for(String pvName : pvNames) {
        	boolean isField = PVNames.isFieldOrFieldModifier(pvName);
        	String plainPVName = PVNames.channelNamePVName(pvName);
        	String fieldName = PVNames.getFieldName(pvName);
			String realName = configService.getRealNameForAlias(plainPVName);
			if(realName != null) {
				plainPVName = realName;
			}
			pvnps.add(new PVNameParts(pvName, plainPVName, isField, fieldName));
		}

        Map<String, String[]> pvFieldsForPVNames = configService.queryPVTypeInfos(
			Predicates.in("__key", pvnps.stream().map((x) -> x.pvName).collect(Collectors.toList()).toArray(new String[0])),
            new FieldsProjection())
			.stream().collect(Collectors.toMap(OnlyFields::pvName, OnlyFields::archiveFields));

		Map<String, String[]> pvFieldsForPlainPVNames = configService.queryPVTypeInfos(
            Predicates.in("__key", pvnps.stream().map((x) -> x.plainPVName).collect(Collectors.toList()).toArray(new String[0])),
            new FieldsProjection())
			.stream().collect(Collectors.toMap(OnlyFields::pvName, OnlyFields::archiveFields));
		
		String[] emptyFields = new String[0];
		LinkedList<String> archivedPVs = new LinkedList<String>();
		for(PVNameParts pvnp : pvnps) {
			if(pvnp.isField) {
				if(pvFieldsForPVNames.containsKey(pvnp.pvName)
				|| Arrays.asList(pvFieldsForPVNames.getOrDefault(pvnp.pvName, emptyFields)).contains(pvnp.fieldName)
				|| pvFieldsForPlainPVNames.containsKey(pvnp.pvName)
				|| Arrays.asList(pvFieldsForPlainPVNames.getOrDefault(pvnp.pvName, emptyFields)).contains(pvnp.fieldName)
				) {
					archivedPVs.add(pvnp.pvName);
				}
			} else {
				if(pvFieldsForPVNames.containsKey(pvnp.pvName)
				|| pvFieldsForPlainPVNames.containsKey(pvnp.pvName)
				) {
					archivedPVs.add(pvnp.pvName);
				}	
			}
		}
		return archivedPVs;
	}
    
}
