const archstatpVNames = "#archstatpVNames";

/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/

// Convert a list of PVs as typed in the #archstatpVNames textarea into a "pv=CSV" type HTTP argument.

function getPVQueryParam() {
  const pvText = $(archstatpVNames).val();
  // Check for value, non-zero length and non-blanks
  if (!pvText || 0 === pvText.length || /^\s*$/.test(pvText)) {
    alert("No PVs have been specified.");
    return;
  }
  const pvS = pvText.split("\n");
  const jsonArray = [];

  for (let i = 0; i < pvS.length; i++) {
    if (!pvS[i] || 0 === pvS[i] || /^\s*$/.test(pvS[i])) continue;
    jsonArray.push({ pv: pvS[i].trim() });
  }
  return JSON.stringify(jsonArray);
}

const patternForPVNames = /^[a-zA-Z0-9:_\-+\[\];\/,#{}^<>]+$/;
// Validates pvNames to make sure they are valid - pattern from CA developers guide.
// Should match pattern in PVNames.isValidPVName
function validatePVNames() {
  const pvText = $(archstatpVNames).val();
  // Check for value, non-zero length and non-blanks
  if (!pvText || 0 === pvText.length || /^\s*$/.test(pvText)) {
    alert("No PVs have been specified for archiving.");
    return false;
  }

  const pvS = pvText.split("\n");
  let message = String("The following PV names do not match the CA spec");
  let errors = false;
  for (let i = 0; i < pvS.length; i++) {
    if (!pvS[i] || 0 === pvS[i] || /^\s*$/.test(pvS[i])) continue;
    let baseName = pvS[i].split(".", 2)[0];
    if (!patternForPVNames.test(baseName.trim())) {
      message = message.concat("\n" + pvS[i]);
      errors = true;
    }
  }
  if (errors) {
    alert(message);
    return false;
  }

  return true;
}

// Archive the list of PVs as typed in the #archstatpVNames textarea
function archivePVs() {
  if (!validatePVNames()) {
    return;
  }
  var pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  $.ajax({
    url: "../bpl/archivePV",
    dataType: "json",
    data: pvQuery,
    type: "POST",
    contentType: "application/json",
    success: function () {
      checkPVStatus();
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while requesting these PVs to be archived -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// @begin(minimumSamplingPeriod)
var minimumSamplingPeriod = 0.1;
// @end(minimumSamplingPeriod)

// Archive the list of PVs as typed in the #archstatpVNames textarea but first popup a dialog that asks for archiving details.
function archivePVsWithDetails() {
  if (!validatePVNames()) {
    return;
  }

  $("#pvDetailsPolicies").empty();
  $("#pvDetailsPolicies").append(
    $('<option value=" " selected="selected">Select</option>')
  );
  $.ajax({
    url: "../bpl/getPolicyList",
    dataType: "json",
    async: false,
    success: function (data, textStatus, jqXHR) {
      for (var name in data) {
        var description = data[name];
        $("#pvDetailsPolicies").append(
          $("<option>", { value: name }).text(description)
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting the list of policies -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });

  $("#pvDetailsParams").show();
  $("#pvDetailsChangeParamDiv").dialog({
    height: 250,
    width: 600,
    modal: true,
  });

  $("#pvDetailsParamsOk").click(function () {
    var samplingMethod = $("#pvDetailsSamplingMethod").val();
    var samplingPeriod = $("#pvDetailsSamplingPeriod").val();
    var controllingPV = $("#pvDetailsControllingPV").val();
    var policySelected = $("#pvDetailsPolicies").val();

    var samplingPeriodParam = "";
    if (
      samplingPeriod == null ||
      samplingPeriod == undefined ||
      samplingPeriod.length <= 0
    ) {
      // No sampling period period is selected.
    } else {
      var patternForFloat = /^[0-9]+(.[0-9]+)?$/;
      if (!patternForFloat.test(samplingPeriod)) {
        alert(
          "The sampling period should be between " +
            minimumSamplingPeriod +
            " and 86400"
        );
        return;
      }
      var samplingPeriodFl = parseFloat(samplingPeriod);
      if (
        samplingPeriodFl < minimumSamplingPeriod ||
        samplingPeriodFl > 86400 ||
        samplingPeriodFl == 0
      ) {
        alert(
          "The sampling period should be a non-zero number between " +
            minimumSamplingPeriod +
            " and 86400"
        );
        return;
      }
      samplingPeriodParam = "&samplingperiod=" + samplingPeriod;
    }

    var samplingMethodParam = "";
    if (
      samplingMethod == null ||
      samplingMethod == undefined ||
      samplingMethod.length <= 0
    ) {
    } else {
      samplingMethodParam = "&samplingmethod=" + samplingMethod;
    }

    var controllingPVParam = "";
    if (!controllingPV || 0 === controllingPV || /^\s*$/.test(controllingPV)) {
      // No controlling PV
    } else {
      if (!patternForPVNames.test(controllingPV)) {
        alert(controllingPV + " does not satisfy the syntax for PV names");
        return;
      } else {
        controllingPVParam =
          "&controllingPV=" + encodeURIComponent(controllingPV);
      }
    }

    var policyParam = "";
    if (
      policySelected == null ||
      policySelected == undefined ||
      policySelected.trim().length <= 0
    ) {
    } else {
      policyParam = "&policy=" + encodeURIComponent(policySelected.trim());
    }

    $("#pvDetailsParams").hide();
    $("#pvDetailsChangeParamDiv").dialog("close");

    var pvQuery = getPVQueryParam();
    if (!pvQuery) return;
    HTTPMethod = "POST";

    $.ajax({
      url: "../bpl/archivePV",
      dataType: "json",
      data:
        pvQuery +
        samplingPeriodParam +
        samplingMethodParam +
        controllingPVParam +
        policyParam,
      type: HTTPMethod,
      success: function () {
        checkPVStatus();
      },
      error: function (jqXHR, textStatus, errorThrown) {
        alert(
          "An error occured on the server while requesting these PVs to be archived -- " +
            textStatus +
            " -- " +
            errorThrown
        );
      },
    });
  });
}

// We assume that dataobject has a attribute called pvName and that we are in the mgmt/ui context.
var dataRetrievalURL = null;
function quickChartButton(dataobject) {
  var canvasSupported = !!window.HTMLCanvasElement;
  if (!canvasSupported) {
    return "N/A";
  }

  if (dataRetrievalURL == null) {
    $.ajax({
      url: "../bpl/getApplianceInfo",
      dataType: "json",
      async: false,
      success: function (data, textStatus, jqXHR) {
        dataRetrievalURL = data.dataRetrievalURL;
      },
      error: function (jqXHR, textStatus, errorThrown) {
        alert(
          "An error occured on the server while getting the appliance information -- " +
            textStatus +
            " -- " +
            errorThrown
        );
      },
    });
  }

  if (dataRetrievalURL == null || dataRetrievalURL == undefined) {
    alert("Cannot determine data retrieval URL");
  }

  return (
    '<a href="' +
    dataRetrievalURL +
    "/ui/viewer/archViewer.html?pv=" +
    encodeURIComponent(dataobject.pvName) +
    '" ><img class="imgintable" src="comm/img/chart.png"/></a>'
  );
}

function abortArchiveRequestFromDetails(pvName) {
  $.ajax({
    url: "../bpl/abortArchivingPV",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvName),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        checkPVStatus();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "abortArchivingPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server aborting the pv archival request -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

var inRefreshPVStatus = false;
var skipAutoRefresh = false;

// Displays the status of the PVs as typed in the #archstatpVNames textarea in the archstats table.
function checkPVStatus() {
  var pvNames = $(archstatpVNames).val();
  if (sessionStorage && pvNames != null) {
    sessionStorage["archstatpVNames"] = $(archstatpVNames).val();
  }

  const pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  const json = pvQuery;
  let url = "../bpl/getPVStatus?reporttype=short";
  const tabledivname = "archstatsdiv";
  createReportTableDATA(json, url, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    {
      srcAttr: "status",
      label: "Status",
      srcFunction: function (curdata) {
        if (skipAutoRefresh) {
          return curdata.status;
        }
        if (
          curdata.status !== undefined &&
          curdata.status !== "Being archived"
        ) {
          if (!inRefreshPVStatus) {
            inRefreshPVStatus = true;
            window.setTimeout(function () {
              inRefreshPVStatus = false;
              checkPVStatus();
            }, 60 * 1000);
          }
        }
        return curdata.status;
      },
    },
    { srcAttr: "appliance", label: "Appliance" },
    { srcAttr: "connectionState", label: "Connected?" },
    { srcAttr: "isMonitored", label: "Monitored?" },
    { srcAttr: "samplingPeriod", label: "Sampling period" },
    { srcAttr: "lastEvent", label: "Last event" },
    {
      label: "Details",
      sortType: "none",
      srcFunction: function (curdata) {
        if (curdata.pvNameOnly !== undefined) {
          return (
            '<a href="pvdetails.html?pv=' +
            encodeURIComponent(curdata.pvName) +
            '" ><img class="imgintable" src="comm/img/details.png"/></a>'
          );
        } else {
          if (curdata.status == "Initial sampling") {
            return (
              '<a onclick="abortArchiveRequestFromDetails(' +
              "'" +
              curdata.pvName +
              "'" +
              ')" ><img class="imgintable" src="comm/img/edit-delete.png"></a>'
            );
          } else {
            return "N/A";
          }
        }
      },
    },
    {
      label: "Quick chart",
      sortType: "none",
      srcFunction: function (curdata) {
        if (curdata.pvNameOnly !== undefined) {
          return quickChartButton(curdata);
        } else {
          return "N/A";
        }
      },
    },
  ]);
  $("#archstatsdiv").show();
}

//Get a report on the PV's that are currently paused
function getPVNames() {
  //http://172.21.225.187:17665/mgmt/bpl/getMatchingPVsForThisAppliance?regex=.*&limit=1000

  var pvNames = $(archstatpVNames).val();
  if (sessionStorage && pvNames != null) {
    sessionStorage["archstatpVNames"] = $(archstatpVNames).val();
  }

  const pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  let pvName;
  if (pvQuery.length > 0) {
    pvName = pvQuery[0].get("pv");
  } else {
    return;
  }
  const url = "../bpl/getMatchingPVsForThisAppliance?" + pvName + "&limit=-1";
  const tabledivname = "archstatsdiv";
  createReportTable([], url, tabledivname, [
    {
      srcAttr: "pvName",
      label: "PV Name",
      srcFunction: function (curdata) {
        return curdata;
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (curdata) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(curdata) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Delete",
      srcFunction: function (curdata) {
        return (
          '<a onclick="showDialogForDeletePausedPV(' +
          "'" +
          curdata +
          "'" +
          ')" ><img class="imgintable" src="comm/img/edit-delete.png"></a>'
        );
      },
    },
  ]);
  $("#archstatsdiv").show();
  $("#pvStopArchivingOk").click(deletePausedPV);
}

function getPVDetails() {
  var pvName = getQueryParams()["pv"];
  $("#pvDetailsName").text(pvName);
  $.ajax({
    url: "../bpl/getPVDetails",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvName),
    success: function (data, textStatus, jqXHR) {
      var pvDetailsTable = $("#pvDetailsTable > tbody:first");
      pvDetailsTable.children("tr").remove();
      $.each(data, function (index) {
        var curdata = data[index];
        var row =
          "<tr><td>" +
          curdata.name +
          "</td><td>" +
          curdata.value +
          "</td></tr>";
        pvDetailsTable.append(row);
      });
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while PV details -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// @begin(archivePVWorkflowBatchSize)
var archivePVWorkflowBatchSize = 1000;
// @end(archivePVWorkflowBatchSize)

//Get a report on the PV's that never connected since the start of the archiver
function getNeverConnectedPVsReport() {
  var jsonurl = "../bpl/getNeverConnectedPVs";
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "requestTime", label: "Time metainfo request was made" },
    { srcAttr: "startOfWorkflow", label: "Time we started the workflow" },
    { srcAttr: "currentState", label: "Current workflow state" },
    { srcAttr: "appliance", label: "Appliance" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Abort request",
      srcFunction: function (dataobject) {
        return (
          '<a onclick="abortArchiveRequest(' +
          "'" +
          dataobject.pvName +
          "'" +
          ')" ><img class="imgintable" src="comm/img/edit-delete.png"></a>'
        );
      },
    },
  ]);
  $("#reporttablediv").on("dataloaded", function (event) {
    if (
      $("#reporttablediv_table").data("data").length >=
      archivePVWorkflowBatchSize
    ) {
      $("#report_warnings").text(
        "There seem to be many unfulfilled archive PV request's in this facility. " +
          "Note that we have recently introduced throttling of archivePV requests and if we have more than " +
          archivePVWorkflowBatchSize +
          " PV's that are invalid, " +
          "the archive PV's requests  that were issued later will never be fulfilled. " +
          "You should consider aborting some of these requests for PV's that are not connecting and are stuck in the METAINFO_REQUESTED state."
      );
    } else {
      $("#report_warnings").text("");
    }
  });
}

// Display details for PV's that are in the waiting metainfo.
function getMetaGetsReport() {
  var jsonurl = "../bpl/getMetaGets";
  var tabledivname = "reporttablediv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      { srcAttr: "pvName", label: "PV Name" },
      { srcAttr: "isScheduled", label: "Monitoring started?" },
      { srcAttr: "appliance", label: "Appliance" },
      { srcAttr: "scheduleStart", label: "When?" },
      { srcAttr: "timerRemaining", label: "Remaining" },
      { srcAttr: "timerDone", label: "Timer Done" },
      { srcAttr: "usePVAccess", label: "Using pvAccess" },
      { srcAttr: "eventsSoFar", label: "Events so far" },
      { srcAttr: "storageSoFar", label: "Storage so far" },
      { srcAttr: "internalState", label: "Internal State" },
      { srcAttr: "mainMeta", sortType: "none", label: "MainMeta" },
    ],
    { initialSort: 1 }
  );
}

function abortArchiveRequest(pvName) {
  $.ajax({
    url: "../bpl/abortArchivingPV",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvName),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getNeverConnectedPVsReport();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "abortArchivingPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server aborting the pv archival request -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Pause disconnected PV
function pauseDisconnectedPV(pvname) {
  $.ajax({
    url: "../bpl/pauseArchivingPV",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvname),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getCurrentlyDisconnectedPVsReport();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "pauseArchivingPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error pausing the archiving for pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Get a report on the PV's that are currently disconnected
function getCurrentlyDisconnectedPVsReport() {
  var jsonurl = "../bpl/getCurrentlyDisconnectedPVs";
  var tabledivname = "reporttablediv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      { srcAttr: "pvName", label: "PV Name" },
      { srcAttr: "lastKnownEvent", label: "Timestamp of last event" },
      { srcAttr: "connectionLostAt", label: "Connection lost at" },
      { srcAttr: "hostName", label: "Hostname" },
      { srcAttr: "commandThreadID", label: "Context ID" },
      { srcAttr: "internalState", label: "Internal State" },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Pause",
        srcFunction: function (dataobject) {
          return (
            '<a onclick="pauseDisconnectedPV(' +
            "'" +
            dataobject.pvName +
            "'" +
            ')" ><img class="imgintable" src="comm/img/pause.png"></a>'
          );
        },
      },
    ],
    { initialSort: 1 }
  );
}

// Get a report on the PVS based on their event rate
function getEventRateReport(limit) {
  var jsonurl = "../bpl/getEventRateReport?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      { srcAttr: "pvName", label: "PV Name" },
      { srcAttr: "eventRate", sortType: "float", label: "Event Rate" },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Details",
        srcFunction: function (dataobject) {
          return (
            '<a href="pvdetails.html?pv=' +
            encodeURIComponent(dataobject.pvName) +
            '" ><img class="imgintable" src="comm/img/details.png"></a>'
          );
        },
      },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Quick chart",
        srcFunction: function (dataobject) {
          return quickChartButton(dataobject);
        },
      },
    ],
    { initialSort: 1 }
  );
}

//Get a report on the PVS based on their storage rate
function getStorageRateReport(limit) {
  var jsonurl = "../bpl/getStorageRateReport?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      { srcAttr: "pvName", label: "PV Name" },
      {
        srcAttr: "storageRate_KBperHour",
        sortType: "float",
        label: "Storage Rate (KB/hour)",
      },
      {
        srcAttr: "storageRate_MBperDay",
        sortType: "float",
        label: "Storage Rate (MB/day)",
      },
      {
        srcAttr: "storageRate_GBperYear",
        sortType: "float",
        label: "Storage Rate (GB/year)",
      },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Details",
        srcFunction: function (dataobject) {
          return (
            '<a href="pvdetails.html?pv=' +
            encodeURIComponent(dataobject.pvName) +
            '" ><img class="imgintable" src="comm/img/details.png"></a>'
          );
        },
      },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Quick chart",
        srcFunction: function (dataobject) {
          return quickChartButton(dataobject);
        },
      },
    ],
    { initialSort: 1 }
  );
}

//Get a report on the PVS based on their creation time
function getRecentlyAddedPVsReport(limit) {
  var jsonurl = "../bpl/getRecentlyAddedPVs?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    // TODO add sorting by time...
    { srcAttr: "creationTime", label: "Time of creation" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

//Get a report on the PVS based on their modification time
function getRecentlyModifiedPVsReport(limit) {
  var jsonurl = "../bpl/getRecentlyModifiedPVs?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "modificationTime", label: "Time of modification" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

//Get a report on the PVS based on their modification time
function getPVsByStorageConsumedReport(limit) {
  var jsonurl = "../bpl/getPVsByStorageConsumed?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    {
      srcAttr: "storageConsumedInMB",
      sortType: "float",
      label: "Storage Consumed (MB)",
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

// Report for PVs by the number of times we lost and reestablished a connection to the IOC
// Very useful for diagnosing IOCs that are on the brink....
function getPVsByLostConnections(limit) {
  var jsonurl = "../bpl/getLostConnectionsReport?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "lostConnections", label: "Connection drops" },
    { srcAttr: "currentlyConnected", label: "Currently connected" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

//Report for PVs that have not received an update in a while...
function getSilentPVs(limit) {
  var jsonurl = "../bpl/getSilentPVsReport?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "lastKnownEvent", label: "Timestamp of last known event" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

// Report for PVs that have dropped events because of incorrect timestamps...
function getPVsByDroppedEventsTimestamp(limit) {
  var jsonurl = "../bpl/getPVsByDroppedEventsTimestamp?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "eventsDropped", label: "Dropped events" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

// Report for PVs that have dropped events because of buffer overflows...
function getPVsByDroppedEventsBuffer(limit) {
  var jsonurl = "../bpl/getPVsByDroppedEventsBuffer?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "eventsDropped", label: "Dropped events" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

//Report for PVs that have dropped events because of type changes in the underlying PV...
function getPVsByDroppedEventsTypeChange(limit) {
  var jsonurl = "../bpl/getPVsByDroppedEventsTypeChange?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "eventsDropped", label: "Dropped events" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

function getScanBufReport(limit) {
  var jsonurl = "../bpl/getPVsByScanCopyTime?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "scanCopy", label: "Scan Buffer Transfer (ms)" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

function getScanGapReport(limit) {
  var jsonurl = "../bpl/getPVsByMaxTimeBetweenScans?limit=" + limit;
  var tabledivname = "reporttablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "pvName", label: "PV Name" },
    { srcAttr: "instance", label: "Instance" },
    { srcAttr: "maxTimeBetweenScans", label: "Max time between SCANs (ms)" },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Details",
      srcFunction: function (dataobject) {
        return (
          '<a href="pvdetails.html?pv=' +
          encodeURIComponent(dataobject.pvName) +
          '" ><img class="imgintable" src="comm/img/details.png"></a>'
        );
      },
    },
    {
      srcAttr: "pvName",
      sortType: "none",
      label: "Quick chart",
      srcFunction: function (dataobject) {
        return quickChartButton(dataobject);
      },
    },
  ]);
}

function showDialogForDeletePausedPV(pvName) {
  console.log("Deleting paused PV" + pvName);

  // $('#pvStopArchivingParamsDiv').attr('title', 'Are you certain you want to stop archiving PV ' + pvName);
  $("#pvStopArchivingParams").data("pvName", pvName);
  $("#pvStopArchivingDeleteData").attr("checked", false);
  $("#pvStopArchivingParams").show();
  $("#pvStopArchivingParamsDiv").dialog({
    height: 250,
    width: 600,
    modal: true,
    title: "Are you certain you want to stop archiving PV " + pvName,
  });
}

function deletePausedPV() {
  var pvName = $("#pvStopArchivingParams").data("pvName");
  $("#pvStopArchivingParams").removeData("pvName");
  $("#pvStopArchivingParams").hide();
  $("#pvStopArchivingParamsDiv").dialog("close");

  var reportTable = $("#reporttablediv_table");
  if (!reportTable.length) reportTable = $("#archstatsdiv_table");
  deleteRowAndRefresh(reportTable, function (dataObject) {
    return dataobj.pvName ? dataobj.pvName == pvName : dataobj == pvName;
  });

  var deleteData = $("#pvStopArchivingDeleteData").is(":checked");
  $.ajax({
    url: "../bpl/deletePV",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvName) +
      "&deleteData=" +
      encodeURIComponent(deleteData),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        console.log("Successfully deleted data for pv " + pvName);
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "deletePV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error in deletePV for " +
          pvName +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Get a report on the PV's that are currently paused
function getPausedPVsReport() {
  var jsonurl = "../bpl/getPausedPVsReport";
  var tabledivname = "reporttablediv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      { srcAttr: "pvName", label: "PV Name" },
      { srcAttr: "instance", label: "Appliance" },
      { srcAttr: "modificationTime", label: "Info last modified" },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Details",
        srcFunction: function (dataobject) {
          return (
            '<a href="pvdetails.html?pv=' +
            encodeURIComponent(dataobject.pvName) +
            '" ><img class="imgintable" src="comm/img/details.png"></a>'
          );
        },
      },
      {
        srcAttr: "pvName",
        sortType: "none",
        label: "Delete",
        srcFunction: function (dataobject) {
          return (
            '<a onclick="showDialogForDeletePausedPV(' +
            "'" +
            dataobject.pvName +
            "'" +
            ')" ><img class="imgintable" src="comm/img/edit-delete.png"></a>'
          );
        },
      },
    ],
    { initialSort: 1 }
  );
  $("#pvStopArchivingOk").click(deletePausedPV);
}

function getApplianceMetrics() {
  var jsonurl = "../bpl/getApplianceMetrics";
  var tabledivname = "metricsappliancesdiv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      {
        srcAttr: "instance",
        label: "Instance Name",
        srcFunction: function (dataobject) {
          return (
            dataobject.instance +
            '<a href="#" onclick="getApplianceMetricsForAppliance(' +
            "'" +
            dataobject.instance +
            "'" +
            ')"><img class="imginlist" src="comm/img/go-jump.png"></a>'
          );
        },
      },
      { srcAttr: "status", label: "Status" },
      { srcAttr: "pvCount", label: "PV Count" },
      { srcAttr: "connectedPVCount", label: "Connected" },
      { srcAttr: "eventRate", label: "Event Rate" },
      { srcAttr: "dataRateGBPerDay", label: "Data Rate (GB/day)" },
      {
        srcAttr: "formattedWriteThreadSeconds",
        label: "Engine write thread(s)",
      },
      { srcAttr: "maxETLPercentage", label: "Max ETL(%)" },
    ],
    { initialSort: 1 }
  );
  // createReportTable should have created a table named tabledivname_table and added a variable called data that contains the JSON array that comes from the server.
  $("#metricsappliancesdiv_table").change(function () {
    var instancesdata = $(this).data("data");
    var instancedata = instancesdata[0];
    var instanceidentity = instancedata.instance;
    getApplianceMetricsForAppliance(instanceidentity);
  });
}

function getApplianceMetricsForAppliance(instanceidentity) {
  $("#metricsapplianceName").text(instanceidentity);
  $.ajax({
    url: "../bpl/getApplianceMetricsForAppliance",
    dataType: "json",
    data: "appliance=" + encodeURIComponent(instanceidentity),
    success: function (data, textStatus, jqXHR) {
      var detailsTable = $("#metricsappliancedetailstable > tbody:first");
      detailsTable.children("tr").remove();
      $.each(data, function (index) {
        var curdata = data[index];
        var row =
          "<tr><td>" +
          curdata.name +
          "</td><td>" +
          curdata.value +
          "</td></tr>";
        detailsTable.append(row);
      });
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting instance metrics details -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function addExternalChannelArchiverServer(serverUrl, externCAType) {
  $.ajax({
    url: "../bpl/addExternalArchiverServer",
    dataType: "json",
    data:
      "externalarchiverserverurl=" +
      encodeURIComponent(serverUrl) +
      "&externalServerType=" +
      encodeURIComponent(externCAType),
    success: function (data, textStatus, jqXHR) {
      if (data.desc != null && data.desc != undefined) {
        if (externCAType == "CA_XMLRPC") {
          $("#addchannelarchivermsg").text(
            "We were able to establish a connection to the external Channel Archiver Data Server at " +
              serverUrl +
              ". Please select the archives you want to serve."
          );
          for (var i = 0; i < data.archives.length; i++) {
            var archive = data.archives[i];
            $("#addchannelarchiverarchives").append(
              $("<option>", { value: archive.key }).text(archive.name)
            );
          }
          $("#addchannelarchiverGetUrl").hide();
          $("#addchannelarchiversuccess").show();
        } else {
          $("#addchannelarchivermsg").text(
            "We were able to establish a connection to the external EPICS Archiver Appliance at " +
              serverUrl +
              "."
          );
          $.ajax({
            url: "../bpl/addExternalArchiverServerArchives",
            dataType: "json",
            data:
              "channelarchiverserverurl=" +
              encodeURIComponent(serverUrl) +
              "&archives=pbraw",
            success: function () {
              $("#addchannelarchiversuccess").hide();
              $("#addchannelarchiverurl").val("");
              $("#addchannelarchiverarchives").empty();
              $("#addchannelarchiverdialog").dialog("close");
              showExternalCAListView();
            },
            error: function (jqXHR, textStatus, errorThrown) {
              alert("An error occured on the server");
            },
          });
        }
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "addExternalArchiverServer returned something valid but did not have a desc field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error adding the external channel archiver server -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function showExternalCAListView() {
  var jsonurl = "../bpl/getExternalArchiverServers";
  var tabledivname = "externalCAlistview";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "CAUrl", label: "URL" },
    { srcAttr: "indexes", label: "Indexes" },
    {
      srcAttr: "CAUrl",
      sortType: "none",
      label: "Delete",
      srcFunction: function (dataobject) {
        return (
          '<a onclick="removeCAServer(' +
          "'" +
          dataobject.CAUrl +
          "','" +
          dataobject.indexes +
          "'" +
          ')" ><img class="imgintable" src="comm/img/edit-delete.png"></a>'
        );
      },
    },
  ]);
}

function selectExternalChannelArchiveServerArchives(serverUrl) {
  var selectedArchives = $("#addchannelarchiverarchives").val();
  if (
    selectedArchives == null ||
    selectedArchives == undefined ||
    selectedArchives.length <= 0
  ) {
    alert("Please select the archives you want to serve.");
    return;
  }

  $.ajax({
    url: "../bpl/addExternalArchiverServerArchives",
    dataType: "json",
    data:
      "channelarchiverserverurl=" +
      encodeURIComponent(serverUrl) +
      "&archives=" +
      selectedArchives.toString(),
    success: function () {
      $("#addchannelarchiversuccess").hide();
      $("#addchannelarchiverurl").val("");
      $("#addchannelarchiverarchives").empty();
      $("#addchannelarchiverdialog").dialog("close");
      showExternalCAListView();
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert("An error occured on the server");
    },
  });
}

function caCompare() {
  var pv = $("#cacomparepvname").val();
  var caserverURL = $("#cacompareserverURL").val();
  var caarchiveindex = $("#cacomparearchiveIndex").val();

  if (pv == null || pv == undefined || pv == "") {
    alert("Please enter a PV name");
    return;
  }
  if (caserverURL == null || caserverURL == undefined || caserverURL == "") {
    alert("Please enter the URL of the Channel Archiver Data Server");
    return;
  }
  if (
    caarchiveindex == null ||
    caarchiveindex == undefined ||
    caarchiveindex == ""
  ) {
    alert(
      "Please enter the index number of the Channel Archiver index to use; this is typically 1"
    );
    return;
  }

  var jsonurl =
    "../bpl/test/compareWithChannelArchiver" +
    "?pv=" +
    encodeURIComponent(pv) +
    "&serverURL=" +
    encodeURIComponent(caserverURL) +
    "&archiveKey=" +
    encodeURIComponent(caarchiveindex);
  var tabledivname = "cacomparetablediv";
  createReportTable(jsonurl, tabledivname, [
    { srcAttr: "src", label: "Source" },
    { srcAttr: "ts", label: "TimeStamp" },
    { srcAttr: "nanos", label: "Nanos" },
    { srcAttr: "sevr", label: "Severity" },
    { srcAttr: "stat", label: "Status" },
  ]);
}

// Change the archival parameters for a given PV.
function changeArchivalParams() {
  var pvname = $("#pvDetailsName").text();
  var samplingMethod = $("#pvDetailsSamplingMethod").val();
  var samplingperiod = $("#pvDetailsSamplingPeriod").val();
  $.ajax({
    url: "../bpl/changeArchivalParameters",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvname) +
      "&samplingperiod=" +
      encodeURIComponent(samplingperiod) +
      "&samplingmethod=" +
      samplingMethod,
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "changeArchivalParameters returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error changing the archival parameters for pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function getStorageMetrics() {
  var jsonurl = "../bpl/getStorageMetrics";
  var tabledivname = "storageappliancesdiv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      {
        srcAttr: "instance",
        label: "Instance Name",
        srcFunction: function (dataobject) {
          return (
            dataobject.instance +
            '<a href="#" onclick="getStorageMetricsForAppliance(' +
            "'" +
            dataobject.instance +
            "'" +
            ')"><img class="imginlist" src="comm/img/go-jump.png"></a>'
          );
        },
      },
      { srcAttr: "status", label: "Status" },
      { srcAttr: "pvCount", label: "PV Count" },
      { srcAttr: "eventRate", label: "Event Rate" },
      { srcAttr: "dataRateGBPerDay", label: "Data Rate (GB/day)" },
      { srcAttr: "capacityUtilized", label: "Capacity consumed" },
    ],
    { initialSort: 1 }
  );
  // createReportTable should have created a table named tabledivname_table and added a variable called data that contains the JSON array that comes from the server.
  $("#storageappliancesdiv_table").change(function () {
    var instancesdata = $(this).data("data");
    var instancedata = instancesdata[0];
    var instanceidentity = instancedata.instance;
    getStorageMetricsForAppliance(instanceidentity);
  });
}

function getStorageMetricsForAppliance(instanceidentity) {
  $("#storageapplianceName").text(instanceidentity);
  $.ajax({
    url: "../bpl/getStorageMetricsForAppliance",
    dataType: "json",
    data: "appliance=" + encodeURIComponent(instanceidentity),
    success: function (data, textStatus, jqXHR) {
      var detailsTable = $("#storageappliancedetailstable > tbody:first");
      detailsTable.children("tr").remove();
      $.each(data, function (index) {
        var curdata = data[index];
        var row =
          "<tr><td>" +
          curdata.name +
          "</td><td>" +
          curdata.total_space +
          "</td><td>" +
          curdata.available_space +
          "</td><td>" +
          curdata.available_space_percent +
          "</td></tr>";
        detailsTable.append(row);
      });
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting instance storage details -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function getInstanceMetrics() {
  var jsonurl = "../bpl/getInstanceMetrics";
  var tabledivname = "instanceappliancesdiv";
  createReportTable(
    jsonurl,
    tabledivname,
    [
      {
        srcAttr: "instance",
        label: "Instance Name",
        srcFunction: function (dataobject) {
          return (
            dataobject.instance +
            '<a href="#" onclick="getInstanceMetricsForAppliance(' +
            "'" +
            dataobject.instance +
            "'" +
            ')"><img class="imginlist" src="comm/img/go-jump.png"></a>'
          );
        },
      },
      { srcAttr: "status", label: "Status" },
      { srcAttr: "pvCount", label: "PV Count" },
      { srcAttr: "connectedPVCount", label: "Connected" },
      { srcAttr: "MGMT_uptime", label: "Mgmt Uptime" },
    ],
    { initialSort: 1 }
  );
  // instanceappliancesdiv should have created a table named tabledivname_table and added a variable called data that contains the JSON array that comes from the server.
  $("#instanceappliancesdiv_table").change(function () {
    var instancesdata = $(this).data("data");
    var instancedata = instancesdata[0];
    var instanceidentity = instancedata.instance;
    getInstanceMetricsForAppliance(instanceidentity);
  });
}

function getInstanceMetricsForAppliance(instanceidentity) {
  var options = {
    lines: { show: true },
    points: { show: false },
    legend: { position: "nw" },
    xaxis: { mode: "time" },
    yaxis: { panRange: [-10, 10] },
    pan: { interactive: false },
    grid: { show: true },
  };

  $("#instanceapplianceName").text(instanceidentity);
  $.ajax({
    url: "../bpl/getProcessMetricsDataForAppliance",
    dataType: "json",
    data: "appliance=" + encodeURIComponent(instanceidentity),
    success: function (data, textStatus, jqXHR) {
      $.plot($("#instancechartplaceholder"), data, options);
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting instance details -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function getPolicyTextFromServer(editor) {
  $.ajax({
    url: "../bpl/getPolicyText",
    dataType: "text",
    success: function (data, textStatus, jqXHR) {
      editor.getSession().setValue(data);
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting the policy text -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Returns the PVtypeInfo for a PV after making a server side call.
//Note that this a syncronous call and will lock the browser for the duration of the call
function getPVTypeInfo(pvName) {
  var pvTypeInfo = null;
  $.ajax({
    url: "../bpl/getPVTypeInfo",
    dataType: "json",
    async: false,
    data: "pv=" + encodeURIComponent(pvName),
    success: function (data) {
      pvTypeInfo = data;
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while requesting detailed information for pv " +
          pvName +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
  return pvTypeInfo;
}

function getStoresForPV(pvName) {
  var stores = null;
  $.ajax({
    url: "../bpl/getStoresForPV",
    dataType: "json",
    async: false,
    data: "pv=" + encodeURIComponent(pvName),
    success: function (data) {
      stores = data;
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while requesting detailed information for pv " +
          pvName +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
  return stores;
}

// Pause archiving PV
function pauseArchivingPV() {
  var pvname = $("#pvDetailsName").text();
  $.ajax({
    url: "../bpl/pauseArchivingPV",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvname),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "pauseArchivingPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error pausing the archiving for pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// Resume archiving PV
function resumeArchivingPV() {
  var pvname = $("#pvDetailsName").text();
  $.ajax({
    url: "../bpl/resumeArchivingPV",
    dataType: "json",
    data: "pv=" + encodeURIComponent(pvname),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "resumeArchivingPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error resuming the archiving for pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Consolidate data for PV
function consolidateDataForPV() {
  var pvname = $("#pvDetailsName").text();
  var storeName = $("#pvConsolidateStore").val();
  $.ajax({
    url: "../bpl/consolidateDataForPV",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvname) +
      "&storage=" +
      encodeURIComponent(storeName),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "consolidateDataForPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error consolidating the data pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

//Stop archiving PV; after we are done with this, we redirect to the home page...
function deletePV() {
  var pvname = $("#pvDetailsName").text();
  var deleteData = $("#pvStopArchivingDeleteData").is(":checked");
  $.ajax({
    url: "../bpl/deletePV",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvname) +
      "&deleteData=" +
      encodeURIComponent(deleteData),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        window.location.href = "index.html";
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "deletePV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error in deletePV for " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// Change the name of the PV to a new name.
function renamePV() {
  var pvname = $("#pvDetailsName").text();
  var newName = $("#pvRenameParamsNewName").val();
  $.ajax({
    url: "../bpl/renamePV",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvname) +
      "&newname=" +
      encodeURIComponent(newName),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "renamePV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured renaming the pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function getNumberOfAppliances() {
  totalAppliances = 1;
  $.ajax({
    url: "../bpl/getApplianceMetrics",
    dataType: "json",
    async: false,
    success: function (data, textStatus, jqXHR) {
      totalAppliances = data.length;
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting the list of appliances -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
  return totalAppliances;
}

function getAppliancesAvailableForResharding(pvName) {
  typeInfo = getPVTypeInfo(pvName);
  currentAppliance = typeInfo.applianceIdentity;
  appliancesList = new Array();
  $.ajax({
    url: "../bpl/getApplianceMetrics",
    dataType: "json",
    async: false,
    success: function (appliances, textStatus, jqXHR) {
      appliancecount = 0;
      for (appliance in appliances) {
        identity = appliances[appliance].instance;
        if (identity != currentAppliance) {
          appliancesList[appliancecount] = appliances[appliance].instance;
          appliancecount++;
        }
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting the list of appliances -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
  return appliancesList;
}

// Change the appliance archiving the PV.
function reshardPV() {
  var pvname = $("#pvDetailsName").text();
  var storeName = $("#pvReshardStore").val();
  var appliance = $("#pvReshardNewAppliance").val();
  $.ajax({
    url: "../bpl/reshardPV",
    dataType: "json",
    data:
      "pv=" +
      encodeURIComponent(pvname) +
      "&storage=" +
      encodeURIComponent(storeName) +
      "&appliance=" +
      encodeURIComponent(appliance),
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVDetails();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "reshardPV returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error resharding the data pv " +
          pvname +
          " -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// Looks up the PVs in #archstatpVNames text area and then replace the text area with the PV names.
function lookupPVs() {
  const pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  const url = "../bpl/getPVStatus?reporttype=short";
  let HTTPMethod = "POST";

  $.ajax({
    url: url,
    data: pvQuery,
    type: HTTPMethod,
    dataType: "json",
    contentType: "application/json",
    success: function (data, textStatus, jqXHR) {
      $(archstatpVNames).val("");
      for (const pvInfo in data) {
        var newval = $(archstatpVNames).val() + data[pvInfo]["pvName"] + "\n";
        $(archstatpVNames).val(newval);
        var pvNames = $(archstatpVNames).val();
        if (sessionStorage && pvNames != null) {
          sessionStorage["archstatpVNames"] = $(archstatpVNames).val();
        }
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while looking up PVs -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function pauseMultiplePVs() {
  const pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  const url = "../bpl/pauseArchivingPV";

  $.ajax({
    url: url,
    data: pvQuery,
    type: "POST",
    dataType: "json",
    contentType: "application/json",
    success: function (data, textStatus, jqXHR) {
      checkPVStatus();
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while pausing PVs -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function deleteMultiplePVs() {
  var pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  var url = "../bpl/deletePV";
  var HTTPMethod = "POST";

  $.ajax({
    url: url,
    data: pvQuery,
    type: HTTPMethod,
    dataType: "json",
    contentType: "application/json",
    success: function (data, textStatus, jqXHR) {
      if (
        data.status != null &&
        data.status != undefined &&
        data.status == "ok"
      ) {
        getPVNames();
      } else if (data.validation != null && data.validation != undefined) {
        alert(data.validation);
      } else {
        alert(
          "deleteMultiplePVs returned something valid but did not have a status field."
        );
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while deleting PVs -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function resumeMultiplePVs() {
  const pvQuery = getPVQueryParam();
  if (!pvQuery) return;

  const url = "../bpl/resumeArchivingPV";
  const HTTPMethod = "POST";

  $.ajax({
    url: url,
    data: pvQuery,
    type: HTTPMethod,
    dataType: "json",
    contentType: "application/json",
    success: function (data, textStatus, jqXHR) {
      checkPVStatus();
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while pausing PVs -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

// Meant for showing the versions in the index page.
function showVersions() {
  $.ajax({
    url: "../bpl/getVersions",
    dataType: "json",
    success: function (data, textStatus, jqXHR) {
      $("#archapplversions").text("EPICS " + data["mgmt_version"]);
      if ("components_with_different_versions" in data) {
        $("#archapplversions").css("color", "red");
      }
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while getting the versions -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function removeCAServer(serverURL, indexes) {
  $.ajax({
    url:
      "../bpl/removeExternalArchiverServer?channelarchiverserverurl=" +
      encodeURIComponent(serverURL) +
      "&archives=" +
      encodeURIComponent(indexes),
    dataType: "json",
    success: function (data, textStatus, jqXHR) {
      if (data.status == "ok") {
        alert(
          "The external archiver server was removed successfully. You may have to restart the entire cluster for this to take effect."
        );
      } else {
        alert(
          "There was some issue removing the external server. Please check the logs for more details.."
        );
      }
      showExternalCAListView();
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "There was some issue removing the external server. Please check the logs for more details.." +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}
