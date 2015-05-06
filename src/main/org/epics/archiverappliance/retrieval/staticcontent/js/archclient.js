/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
// The main javascript file for integration with the appliance archiver.

// TODO turn on show points if the total points is small.
var options = {
 lines: { show: true },
 points: { show: false },
 xaxis: { mode: "time" },
 yaxis: { panRange: [-10, 10] },
 pan: { interactive: true },
 grid: {show: true }
};


var placeholder = $("#chartplaceholder");

function toggleReduced() {
	if(usereduced=='true') {
		usereduced = 'false';
	} else {
		usereduced = 'true';
	}
}

var currentChartData = null;
function onDataReceived(pvdata) {
    currentChartData = pvdata;
    $.plot(placeholder, pvdata, options);
}

// show pan messages to illustrate events 
placeholder.bind('plotpan', function (event, plot) {
    var axes = plot.getAxes();
    $("#message").html("Panning to x: "  + (new Date(axes.xaxis.min)).format("JODAISODateTime")
                       + " &ndash; " + (new Date(axes.xaxis.max)).format("JODAISODateTime")
                       + " and y: " + axes.yaxis.min.toFixed(2)
                       + " &ndash; " + axes.yaxis.max.toFixed(2)
                       );
});

placeholder.bind('panend', function (event, plot) {
    var axes = plot.getAxes();
	params.from = (new Date(axes.xaxis.min)).format("JODAISODateTime");
	params.to = (new Date(axes.xaxis.max)).format("JODAISODateTime");
    $.getJSON("../data/getData.jplot", params, onDataReceived);
});
    

$(function () {
	currentChartData = null;
    $.getJSON("../data/getData.jplot", params, onDataReceived);
});


