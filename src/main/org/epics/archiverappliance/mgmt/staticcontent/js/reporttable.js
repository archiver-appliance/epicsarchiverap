/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
// Poor mans report table with paging, filtering and sorting thrown in
// This is designed for usecases where the server cannot support a consistent view of a report (such as where the items in the report are being determined from runtime information that changes all the time).
// We expect very minimal support from the server and the heavy lifting is done on the client side; so please don't use this if you are expecting to transport millions of objects with millions of attributes across a WAN.
// Main function is a createReportTable that takes in a jsonurl, the id of an empty div to host the table, row and column definitions.
// JSON call is expected to return an array of simple objects (no object graphs for now).
// This JSON returned is then stored into the table itself as an expando and all subsequent paging/filtering/sorting are driven off this expando.
// There is a refresh button on the toolbar that refetches the data from the server.
// Column definitions are expected to be an array of simple objects, one per column
// Column definitions have these attributes
// 1) srcAttr (mandatory) - identifies the attribute in each of the JSON objects used to determine value for this column.
//   For example, if incoming JSON from server is [{"requestTime":"Nov\/10\/2011 14:35:20 PST","pvName":"mshankar:arch:sine88"}],
//   then a coldefs of [{'srcAttr' : 'pvName'} , {'srcAttr' : 'requestTime'}] fills out the first column with all the pvName's and the second column with all the requestTimes
// 2) label (mandatory) - The label to be used for this column.
// 3) sortType (optional) - identifies the sorting function to be used. By default, we use string's localeCompare.
// However, we support 'float' for floating point.
// A sorttype of 'none' does not add the sorting functionality
// 4) srcFunction - If a srcFunction is defined, then the text that this function returns is inserted as the HTML for this column.
// The row definition is optional and is a simple object
// The row definition can have these attributes
// 1) initialSort (optional) - This is the column index of the column to sort the data by after fetching the data from the server.
// 2) initialSortDirection (optional) - This is the direction of the sort. By default, we do a descending sort. If this attribute has the value 'asc', then we do an ascending sort.

function createReportTable(jsonurl, tabledivname, coldefs, rowdefs) {
  var reporttablediv = $("#" + tabledivname);
  reporttablediv.empty();
  var reportTable = $(
    '<table id="' + tabledivname + '_table" class="reporttable">'
  );
  reportTable
    .append(
      $("<thead>").append(
        $('<tr class="toolbar">').append(
          $('<th colspan="3">').append(
            $('<span class="toolbartopspan"/>').append(
              '<img src="../ui/comm/img/search.png"/>',
              '<span class="toolbarsep"/>',
              '<select><option value="10">10</option><option value="25" selected="selected">25</option><option value="50">50</option><option value="100">100</option></select>',
              '<span class="toolbarsep"/>',
              '<img src="../ui/comm/img/go-first.png"/>',
              '<img src="../ui/comm/img/go-previous.png"/>',
              '<span class="toolbarpagenum">Page 1 of 10</span>',
              '<img src="../ui/comm/img/go-next.png"/>',
              '<img src="../ui/comm/img/go-last.png"/>',
              '<span class="toolbarsep"/>',
              '<img src="../ui/comm/img/view-refresh.png"/>',
              '<span class="toolbarright"/>'
            )
          )
        ),
        $('<tr class="colnames">')
      )
    )
    .append("<tbody>");
  reportTable.data("coldefs", coldefs);
  if (rowdefs != null && rowdefs != undefined)
    reportTable.data("rowdefs", rowdefs);
  reportTable.data("jsonurl", jsonurl);
  reporttablediv.append(reportTable);

  getJSONDataAndRefreshTable(reportTable);

  reportTable.data("paging", { pagesize: 25, currentpage: 0 });

  for (var c in coldefs) {
    reportTable.find("thead tr:eq(1)").append($("<th>").text(coldefs[c].label));
    addSorterToTableTh(reportTable, c);
  }

  setupToolbarActions(reportTable);
}

// This assumes the incoming table jQuery object has an attribute called data.
// It then adds a row for each object in data.
// It also assumes that the column definitions are to be found in another attribute called coldefs.
// For now the column definitions are objects with one attribute called srcAttr that identifies the attribute of each object in data to be used in filling the table.
function addRowsForData(tableobj) {
  var dataobjs = tableobj.data("data");
  if (dataobjs == null || dataobjs == undefined) return;
  var coldefs = tableobj.data("coldefs");
  if (coldefs == null || coldefs == undefined) return;
  var paginginfo = tableobj.data("paging");
  var curpage = 0;
  var pagesize = 50;
  if (paginginfo != null && paginginfo != undefined) {
    curpage = paginginfo.currentpage;
    pagesize = paginginfo.pagesize;
  }

  if (dataobjs.length == 0) {
    tableobj.append(
      '<tr><td colspan="' + coldefs.length + '">No data found</td></tr>'
    );
  }

  for (
    var r = curpage * pagesize;
    r < dataobjs.length && r < (curpage + 1) * pagesize;
    r++
  ) {
    var dataobj = dataobjs[r];
    var rowstr = "<tr>";
    for (var c in coldefs) {
      var coldef = coldefs[c];
      if (coldef.srcFunction != null && coldef.srcFunction != undefined) {
        rowstr = rowstr + "<td>" + coldef.srcFunction(dataobj) + "</td>";
      } else {
        var textforrow = dataobj[coldef.srcAttr];
        if (textforrow == undefined || textforrow == null) {
          textforrow = "N/A";
        }
        rowstr = rowstr + "<td>" + textforrow + "</td>";
      }
    }
    rowstr = rowstr + "</tr>";
    tableobj.append(rowstr);
  }

  // MSI8 hack; why do we have to keep doing stuff like this?
  if ($.browser.msie) {
    tableobj.find("tbody tr:nth-child(even)").addClass("rtbleven");
  }
}

var sortingfunctions = new Object();
// Wanted to avoid this global variable but am unable to get functions returning functions working in chrome.
var sortingcurrattr = null;

function stringasc(x, y) {
  if (x[sortingcurrattr] == null || x[sortingcurrattr] == undefined) return -1;
  if (y[sortingcurrattr] == null || y[sortingcurrattr] == undefined) return 1;
  return x[sortingcurrattr].localeCompare(y[sortingcurrattr]);
}

function stringdesc(x, y) {
  if (x[sortingcurrattr] == null || x[sortingcurrattr] == undefined) return 1;
  if (y[sortingcurrattr] == null || y[sortingcurrattr] == undefined) return -1;
  return y[sortingcurrattr].localeCompare(x[sortingcurrattr]);
}

function floatasc(x, y) {
  if (x[sortingcurrattr] == null || x[sortingcurrattr] == undefined) return -1;
  if (y[sortingcurrattr] == null || y[sortingcurrattr] == undefined) return 1;
  return parseFloat(x[sortingcurrattr]) - parseFloat(y[sortingcurrattr]);
}

function floatdesc(x, y) {
  if (x[sortingcurrattr] == null || x[sortingcurrattr] == undefined) return 1;
  if (y[sortingcurrattr] == null || y[sortingcurrattr] == undefined) return -1;
  return parseFloat(y[sortingcurrattr]) - parseFloat(x[sortingcurrattr]);
}

sortingfunctions["string_asc"] = stringasc;
sortingfunctions["string_desc"] = stringdesc;
sortingfunctions["float_asc"] = floatasc;
sortingfunctions["float_desc"] = floatdesc;

// Adds a sorted to the specified tables th element
function addSorterToTableTh(reportTable, indexofthelement) {
  var coldefs = reportTable.data("coldefs");
  if (coldefs == null) return;
  var coldef = coldefs[indexofthelement];
  var th = reportTable.find("thead tr:eq(1) th").eq(indexofthelement);
  if (
    coldef.sortType != null &&
    coldef.sortType != undefined &&
    coldef.sortType == "none"
  ) {
    th.addClass("archtabl_nosort");
    return;
  }
  th.addClass("archtabl_unsorted");
  th.click(function () {
    var dataobjs = reportTable.data("data");
    if (dataobjs == null) return;
    var coldefs = reportTable.data("coldefs");
    if (coldefs == null) return;
    var sortingfnprefix = "string_";
    if (
      coldefs[indexofthelement].sortType != null &&
      coldefs[indexofthelement].sortType != undefined
    ) {
      sortingfnprefix = coldefs[indexofthelement].sortType + "_";
      if (
        sortingfunctions[sortingfnprefix + "asc"] == undefined ||
        sortingfunctions[sortingfnprefix + "asc"] == null ||
        sortingfunctions[sortingfnprefix + "desc"] == undefined ||
        sortingfunctions[sortingfnprefix + "desc"] == null
      ) {
        alert(
          "No sorting function defined for prefix " +
            sortingfnprefix +
            ". Defaulting to string"
        );
        sortingfnprefix = "string_";
      }
    }
    var sortfunction = sortingfunctions[sortingfnprefix + "asc"];
    if ($(this).hasClass("archtabl_unsorted")) {
      $(this).removeClass("archtabl_unsorted");
      $(this).addClass("archtabl_asc");
      sortfunction = sortingfunctions[sortingfnprefix + "asc"];
    } else if ($(this).hasClass("archtabl_asc")) {
      $(this).removeClass("archtabl_asc");
      $(this).addClass("archtabl_desc");
      sortfunction = sortingfunctions[sortingfnprefix + "desc"];
    } else if ($(this).hasClass("archtabl_desc")) {
      $(this).removeClass("archtabl_desc");
      $(this).addClass("archtabl_asc");
      sortfunction = sortingfunctions[sortingfnprefix + "asc"];
    }
    sortingcurrattr = coldef.srcAttr;
    dataobjs.sort(sortfunction);

    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    }
    curpage = 0;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    updateToolbarPageNum(reportTable);

    reportTable.children("tbody").empty();
    addRowsForData(reportTable);
  });
}

function getJSONDataAndRefreshTable(reportTable) {
  var jsonurl = reportTable.data("jsonurl");
  var components = jsonurl.split("?");
  var urlalone = components[0];
  var querystring = "";
  if (components.length > 1) {
    querystring = components[1];
  }
  var HTTPMethod = "GET";
  if (jsonurl.length > 2048) {
    HTTPMethod = "POST";
  }

  $.ajax({
    url: urlalone,
    data: querystring,
    type: HTTPMethod,
    dataType: "json",
    success: function (data, textStatus, jqXHR) {
      var coldefs = reportTable.data("coldefs");
      if (coldefs == null) return;
      var rowdefs = reportTable.data("rowdefs");
      if (rowdefs != null) {
        var initialSort = rowdefs.initialSort;
        var initialSortDirection = rowdefs.initialSortDirection;
        if (initialSort != null && initialSort != undefined) {
          sortingcurrattr = coldefs[initialSort].srcAttr;
          var sortingfnprefix = "string_";
          if (
            coldefs[initialSort].sortType != null &&
            coldefs[initialSort].sortType != undefined
          ) {
            sortingfnprefix = coldefs[initialSort].sortType + "_";
            if (
              sortingfunctions[sortingfnprefix + "asc"] == undefined ||
              sortingfunctions[sortingfnprefix + "asc"] == null ||
              sortingfunctions[sortingfnprefix + "desc"] == undefined ||
              sortingfunctions[sortingfnprefix + "desc"] == null
            ) {
              alert(
                "No sorting function defined for prefix " +
                  sortingfnprefix +
                  ". Defaulting to string"
              );
              sortingfnprefix = "string_";
            }
          }
          var sortfunction = sortingfunctions[sortingfnprefix + "desc"];
          if (
            initialSortDirection != null &&
            initialSortDirection != undefined
          ) {
            sortfunction = sortingfunctions[sortingfnprefix + "asc"];
          }
          data.sort(sortfunction);
        }
      }

      reportTable.removeData("data");
      reportTable.data("data", data);
      reportTable.removeData("unfiltereddata");
      reportTable.data("unfiltereddata", data);
      addRowsForData(reportTable);
      updateToolbarPageNum(reportTable);

      reportTable.change();

      reportTable.parent().trigger("dataloaded");
    },
    error: function (jqXHR, textStatus, errorThrown) {
      alert(
        "An error occured on the server while disconnected PVs -- " +
          textStatus +
          " -- " +
          errorThrown
      );
    },
  });
}

function updateToolbarPageNum(reportTable) {
  var paginginfo = reportTable.data("paging");
  var curpage = 0;
  var pagesize = 25;
  if (paginginfo != null && paginginfo != undefined) {
    curpage = paginginfo.currentpage;
    pagesize = paginginfo.pagesize;
  } else {
    alert("Assertion failure - Paging info is null.");
  }
  var dataobjs = reportTable.data("data");
  if (dataobjs == null) return;
  var totaldataelements = dataobjs.length;
  var totalPages = Math.floor((totaldataelements - 1) / pagesize) + 1;
  if (totalPages <= 0) totalPages = 1;
  var toolBarPageNumSpan = reportTable
    .find("thead tr.toolbar span:eq(0) span:eq(2)")
    .first();
  toolBarPageNumSpan.text("Page " + (curpage + 1) + " of " + totalPages);
}

function setupToolbarActions(reportTable) {
  var filterButton = reportTable.find("thead tr.toolbar img:eq(0)").first();
  filterButton.click(function () {
    var unfiltereddata = reportTable.data("unfiltereddata");
    if (unfiltereddata == null || unfiltereddata == undefined) return;
    var coldefs = reportTable.data("coldefs");
    if (coldefs == null) return;

    var currentFilter = reportTable.data("currentfilter");
    if (currentFilter == undefined) {
      currentFilter = "";
    }
    var newfilter = prompt("Search for rows containing ", currentFilter);
    reportTable.removeData("currentfilter");
    reportTable.data("currentfilter", newfilter);
    reportTable.removeData("data");

    if (newfilter == null || newfilter == "") {
      reportTable.data("data", unfiltereddata);
    } else {
      var data = new Array();
      for (var x in unfiltereddata) {
        var elem = unfiltereddata[x];
        for (var c in coldefs) {
          var coldef = coldefs[c];
          var attrname = coldef.srcAttr;
          var attr = elem[attrname];
          if (attr != null && attr != undefined) {
            if (attr.indexOf(newfilter) !== -1) {
              data.push(elem);
              break;
            }
          }
        }
      }
      reportTable.data("data", data);
    }

    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    curpage = 0;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    reportTable.children("tbody").empty();
    addRowsForData(reportTable);
    updateToolbarPageNum(reportTable);
  });

  var pageNumSelector = reportTable
    .find("thead tr.toolbar select:eq(0)")
    .first();
  pageNumSelector.change(function () {
    var selectedPageSize = $(this).val();
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    pagesize = parseInt(selectedPageSize);
    curpage = 0;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    reportTable.children("tbody").empty();
    addRowsForData(reportTable);
    updateToolbarPageNum(reportTable);
  });

  var firstbutton = reportTable.find("thead tr.toolbar img:eq(1)").first();
  firstbutton.click(function () {
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    curpage = 0;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    reportTable.children("tbody").empty();
    addRowsForData(reportTable);
    updateToolbarPageNum(reportTable);
  });

  var prevbutton = reportTable.find("thead tr.toolbar img:eq(2)").first();
  prevbutton.click(function () {
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    curpage--;
    if (curpage >= 0) {
      reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
      reportTable.children("tbody").empty();
      addRowsForData(reportTable);
      updateToolbarPageNum(reportTable);
    }
  });

  var nextbutton = reportTable.find("thead tr.toolbar img:eq(3)").first();
  nextbutton.click(function () {
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    curpage++;
    var dataobjs = reportTable.data("data");
    if (dataobjs == null) return;
    var totaldataelements = dataobjs.length;
    var totalPages = Math.floor((totaldataelements - 1) / pagesize) + 1;
    if (curpage < totalPages) {
      reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
      reportTable.children("tbody").empty();
      addRowsForData(reportTable);
      updateToolbarPageNum(reportTable);
    }
  });

  var lastbutton = reportTable.find("thead tr.toolbar img:eq(4)").first();
  lastbutton.click(function () {
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    var dataobjs = reportTable.data("data");
    if (dataobjs == null) return;
    var totaldataelements = dataobjs.length;
    var totalPages = Math.floor((totaldataelements - 1) / pagesize) + 1;
    curpage = totalPages - 1;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    reportTable.children("tbody").empty();
    addRowsForData(reportTable);
    updateToolbarPageNum(reportTable);
  });

  var refreshButton = reportTable.find("thead tr.toolbar img:eq(5)").first();
  refreshButton.click(function () {
    var paginginfo = reportTable.data("paging");
    var curpage = 0;
    var pagesize = 25;
    if (paginginfo != null && paginginfo != undefined) {
      curpage = paginginfo.currentpage;
      pagesize = paginginfo.pagesize;
    } else {
      alert("Assertion failure - Paging info is null.");
    }
    curpage = 0;
    reportTable.data("paging", { pagesize: pagesize, currentpage: curpage });
    reportTable.children("tbody").empty();
    getJSONDataAndRefreshTable(reportTable);
  });
}

// Delete the data row in the table for which matching function returns true and then refresh the current page.
// Can be used in reports to delete rows one after the other.
function deleteRowAndRefresh(reportTable, matchingFunction) {
  var dataobjs = reportTable.data("data");
  for (dindex in dataobjs) {
    dataobj = dataobjs[dindex];
    if (matchingFunction(dataobj)) {
      console.log("Found matching row to delete at index " + dindex);
      dataobjs.splice(dindex, 1);
      break;
    }
  }
  reportTable.children("tbody").empty();
  addRowsForData(reportTable);
  updateToolbarPageNum(reportTable);
}
