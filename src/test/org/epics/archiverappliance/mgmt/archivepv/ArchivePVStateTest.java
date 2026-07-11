package org.epics.archiverappliance.mgmt.archivepv;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.MetaInfo;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.config.UserSpecifiedSamplingParams;
import org.epics.archiverappliance.mgmt.archivepv.ArchivePVState.ArchivePVStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the archive-PV workflow guards that handle a PV being paused or having its
 * typeinfo deleted while the archive request is still in flight (e2e5c8d7, 970077a0).
 *
 * <p>The workflow is driven in-process: we enter at METAINFO_OBTAINED (rather than START, which
 * would post a PubSubEvent the engine must answer), let it settle to POLICY_COMPUTED /
 * TYPEINFO_STABLE, then mutate the config service to reproduce the mid-workflow race.
 */
public class ArchivePVStateTest {

    private ConfigServiceForTests configService;

    @BeforeEach
    public void setUp() throws Exception {
        configService = new ConfigServiceForTests(-1);
    }

    @AfterEach
    public void tearDown() {
        if (configService != null) {
            configService.shutdownNow();
        }
    }

    /**
     * Register an archive request for {@code pvName} (assigned to this appliance so we skip
     * capacity planning against peers) and return the workflow state parked at METAINFO_OBTAINED.
     */
    private ArchivePVState requestParkedAtMetaInfoObtained(String pvName) {
        UserSpecifiedSamplingParams userSpec = new UserSpecifiedSamplingParams();
        userSpec.setSkipCapacityPlanning(true);
        configService.addToArchiveRequests(pvName, userSpec);

        MetaInfo metaInfo = new MetaInfo();
        metaInfo.setArchDBRTypes(ArchDBRTypes.DBR_SCALAR_DOUBLE);

        ArchivePVState state = new ArchivePVState(pvName, configService);
        state.metaInfoObtained(metaInfo);
        return state;
    }

    @Test
    public void pausedMidWorkflowFinishesWithoutArchiving() {
        // Use a plain name so getTypeInfoForPV reflects only what we store (ARCH_UNIT_TEST_ names
        // get a synthesized typeinfo, which would mask deletion in the sibling tests).
        String pvName = "test:archivepv:pausedMidWorkflow";
        ArchivePVState state = requestParkedAtMetaInfoObtained(pvName);

        state.nextStep(); // METAINFO_OBTAINED -> POLICY_COMPUTED
        Assertions.assertEquals(ArchivePVStateMachine.POLICY_COMPUTED, state.getCurrentState());
        state.nextStep(); // POLICY_COMPUTED -> TYPEINFO_STABLE
        Assertions.assertEquals(ArchivePVStateMachine.TYPEINFO_STABLE, state.getCurrentState());

        // The PV is paused while the request is still in flight.
        PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
        typeInfo.setPaused(true);
        configService.updateTypeInfoForPV(pvName, typeInfo);

        state.nextStep(); // TYPEINFO_STABLE (paused) -> FINISHED, without submitting an archive request

        Assertions.assertEquals(
                ArchivePVStateMachine.FINISHED,
                state.getCurrentState(),
                "A PV paused mid-workflow must finish without archiving");
        Assertions.assertNotEquals(
                ArchivePVStateMachine.ARCHIVE_REQUEST_SUBMITTED,
                state.getCurrentState(),
                "A paused PV must not reach ARCHIVE_REQUEST_SUBMITTED");
    }

    @Test
    public void typeInfoDeletedAtPolicyComputedAborts() {
        String pvName = "test:archivepv:deletedAtPolicyComputed";
        ArchivePVState state = requestParkedAtMetaInfoObtained(pvName);

        state.nextStep(); // METAINFO_OBTAINED -> POLICY_COMPUTED
        Assertions.assertEquals(ArchivePVStateMachine.POLICY_COMPUTED, state.getCurrentState());

        // The typeinfo is deleted (PV paused/deleted) before the next tick.
        configService.removePVFromCluster(pvName);

        state.nextStep(); // POLICY_COMPUTED (typeinfo gone) -> ABORTED

        Assertions.assertEquals(ArchivePVStateMachine.ABORTED, state.getCurrentState());
        Assertions.assertTrue(
                state.getAbortReason().contains("deleted mid-workflow"),
                "Abort reason should name the mid-workflow deletion, was: " + state.getAbortReason());
    }

    @Test
    public void typeInfoDeletedAtTypeInfoStableAborts() {
        String pvName = "test:archivepv:deletedAtTypeInfoStable";
        ArchivePVState state = requestParkedAtMetaInfoObtained(pvName);

        state.nextStep(); // METAINFO_OBTAINED -> POLICY_COMPUTED
        state.nextStep(); // POLICY_COMPUTED -> TYPEINFO_STABLE
        Assertions.assertEquals(ArchivePVStateMachine.TYPEINFO_STABLE, state.getCurrentState());

        // The typeinfo is deleted after it had settled but before archiving is submitted.
        configService.removePVFromCluster(pvName);

        state.nextStep(); // TYPEINFO_STABLE (typeinfo gone) -> ABORTED

        Assertions.assertEquals(ArchivePVStateMachine.ABORTED, state.getCurrentState());
        Assertions.assertTrue(
                state.getAbortReason().contains("deleted mid-workflow"),
                "Abort reason should name the mid-workflow deletion, was: " + state.getAbortReason());
    }
}
