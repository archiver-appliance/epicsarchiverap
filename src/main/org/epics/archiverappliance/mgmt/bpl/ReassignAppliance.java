package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * <div>Change the appliance for a PV. This is an alternate approach for resharding a PV that relies on data stores
 * that are shared between appliances and <b>consolidateOnShutdown</b> on multiple data stores.
 * </div>
 *
 * <div> Here's one scenario where reassign applices can be used.</div>
 * <ul>
 * <li>We have a three stages of storage - STS, MTS and LTS.</li>
 * <li>The LTS is shared between the apliances in the cluster. That is, all the appliances in the cluster mount the same storage for the LTS.
 * For example, for the PB file for 2025 for the PV <i>arch:test</i>, <code>arch/test_2025.pb</code>, all appliances will be able to read/write the same LTS file <code>/arch/lts/arch/test_2025.pb</code></li>
 * <li>The PV is assigned to one appliance at a time; so only that appliance will actually read/write from the shared LTS. But, if at any time in the future, the PV is assigned to another appliance; that new appliance can continue to read/write from the shared LTS and there will be no data loss.</li>
 * <li>In such a situation, one cen simple change the {@link org.epics.archiverappliance.config.PVTypeInfo#applianceIdentity applianceIdentity} for the {@link org.epics.archiverappliance.config.PVTypeInfo PVTypeInfo} to reassign the PV to another appliance</li>
 * <li>Since the STS and the MTS are not shared between the appliances in the cluster, we configure both of these with a <b>consolidateOnShutdown</b> to move the data to the LTS when the PV's ETL jobs are shutdown on the source appliance.</li>
 * <li>All the appliance components ( mgmt, ETL, engine and retrieval ) subscribe to changes in PVTypeInfo's and react accordingly.</li>
 * <li>Reassigning PVs tp new appliances is not transactional. Once a PV has been reassigned, plese do check the various components to make sure the reassignment was successful.</li>
 * </ul>
 * <div>Reassigning PVs tp new appliances is relatively inexpensive when compared to resharding. Thus, appliance reassignment can be used for more dynamic capacity planning. It can also be done in bulk.</div>
 *
 * @epics.BPLAction - This BPL changes the applianceIdentity on the PV's PVTypeInfo to another appliance. This BPL assumes that the data stores are configured correctly to support this. Please see the JavaDoc for more details; otherwise this operation will result in data loss.
 * @epics.BPLActionParam pv - The name of the pv. The PV needs to be paused first and will remain in a paused state after the resharding is complete.
 * @epics.BPLActionParam appliance - The new appliance to assign the PV to. This is the same string as the <code>identity</code> element in the <code>appliances.xml</code> that identifies this appliance.
 * @epics.BPLActionEnd
 *
 *
 * @author mshankar
 */
public class ReassignAppliance implements BPLAction {
    private static Logger logger = LogManager.getLogger(ReassignAppliance.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        if (!configService.hasClusterFinishedInitialization()) {
            // If you have defined spare appliances in the appliances.xml that will never come up; you should remove
            // them
            // This seems to be one of the few ways we can prevent split brain clusters from messing up the pv <->
            // appliance mapping.
            throw new IOException(
                    "Waiting for all the appliances listed in appliances.xml to finish loading up their PVs into the cluster");
        }

        if (!Boolean.parseBoolean((String) configService
                .getInstallationProperties()
                .getOrDefault(ReassignAppliance.class.getCanonicalName(), false))) {
            throw new IOException(
                    "This installation has not been configured to support dynamic reassignment of PV's to appliances");
        }

        String destApplianceIdentity = req.getParameter("appliance");
        if (destApplianceIdentity == null || destApplianceIdentity.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        ApplianceInfo destApplianceInfo = configService.getAppliance(destApplianceIdentity);
        if (destApplianceInfo == null) {
            logger.error("Unable to find appliance with identity " + destApplianceIdentity);
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req);
        LinkedHashMap<String, String> statuses = new LinkedHashMap<String, String>();
        for (String pvName : pvNames) {
            String pvNameFromRequest = pvName;
            String realName = configService.getRealNameForAlias(pvName);
            if (realName != null) pvName = realName;

            ApplianceInfo srcApplianceInfo = configService.getApplianceForPV(pvName);
            if (srcApplianceInfo == null) {
                statuses.put(pvNameFromRequest, "Unable to find appliance for PV " + pvName);
                continue;
            }

            if (srcApplianceInfo.getIdentity().equals(destApplianceInfo.getIdentity())) {
                // The various components react to changes in the PVTypeInfoi. THis is not a change; so nothing will
                // happen even if we pwemitted this.
                statuses.put(
                        pvNameFromRequest,
                        "Attempting to reassign onto same appliance - " + srcApplianceInfo.getIdentity() + "/"
                                + destApplianceInfo.getIdentity());
                continue;
            }

            PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
            if (typeInfo == null) {
                statuses.put(pvNameFromRequest, "Unable to find typeinfo for PV " + pvName);
                continue;
            }

            try {
                typeInfo.setApplianceIdentity(destApplianceInfo.getIdentity());
                configService.updateTypeInfoForPV(pvName, typeInfo);
                configService.registerPVToAppliance(pvName, destApplianceInfo);
                statuses.put(pvNameFromRequest, "Success");
            } catch (Exception ex) {
                String msg = "Exception reassiging PV " + pvName + " to appliance " + destApplianceIdentity;
                logger.error(msg);
                statuses.put(pvNameFromRequest, msg);
            }
        }
    }
}
