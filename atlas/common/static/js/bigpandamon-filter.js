/***
 * bigpandamon-filter.js
 ***/
/*
var pgst;
var filter;
var shFilter;
var fields;
var tableid;
var datasrc;
var colsDict = {};
var shFilter = [];
var colsOrig;
var fieldIndices;
var fltr = {'pgst': 'ini'};
var stFlag;
*/

function buildFilterTable(tableid)
{
    $( "#btnFilter-" + tableid).show();
    $( "#CreationFrom" ).datetimepicker({ dateFormat: "yy-mm-dd" });
    $( "#CreationTo" ).datetimepicker({ dateFormat: "yy-mm-dd" });
    $( "#ModificationFrom" ).datetimepicker({ dateFormat: "yy-mm-dd" });
    $( "#ModificationTo" ).datetimepicker({ dateFormat: "yy-mm-dd" });
}

function updateQueryStringParameter(uri, key, value) {
	var re = new RegExp("([?&])" + key + "=.*?(&|$)", "i");
	var separator = uri.indexOf('?') !== -1 ? "&" : "?";
	if (uri.match(re)) {
		return uri.replace(re, '$1' + key + "=" + value + '$2');
	} else {
		return uri + separator + key + "=" + value;
	}
}

function buildSummary(divid, data)
{
	$( "#" + divid).show();
	$( "#smry-title").hide();
	$( "#" + divid).append('<div class="large-12 text-justify"><strong>Summary</strong><br/><br/></div>');
	if (typeof(data.aaData) != 'undefined'){
		var s = '<div class="large-12 columns"><ul>';
		for (var key in data.aaData) {
			s += '<li><span><strong>' +  key + ':</strong>&nbsp;</span>';
			for (var r in data.aaData[key]){
				if (data.aaData[key][r].c != null){
					var smryURL = updateQueryStringParameter(
							window.location.href,
							data.aaData[key][r].f, 
							data.aaData[key][r].c
					)
					s += '<emph>' +  data.aaData[key][r].c + '</emph>' 
						+ '(<a href="' + smryURL + '" target="_blank">' 
						+ data.aaData[key][r].v + "</a>"
						+ ') ';
				} else {
				s+= '<emph>' +  'other' + '</emph>' 
				+ '(' + data.aaData[key][r].v + ') ';
				}
			}
			s+='</li>';
		}
		s+='</ul></div>';
		$( "#" + divid).append(s);
	}
}

function gFV(fieldName)
//gFV ... getFieldValue : get value of input with id===fieldName
{
 return $.trim($("#"+fieldName).val());
}
function sFV(fieldName, val)
//sFV ... setFieldValue : set value of input with id===fieldName
{
 $("#"+fieldName).val(val);
}
function sFVms(fieldName, valList)
//sFVms ... setFieldValue : set option selected for multi select
{
 vals = valList.split(',');
 $("#"+fieldName).val(vals);
}

function setValuesFilterTable(f)
//setValuesFilterTable
{
 for (x in f){
     if(f[x].value.indexOf(',') === -1){
         sFV(f[x].name, f[x].value);
     } else {
         sFVms(f[x].name, f[x].value);
     }
 }
}

function getValuesFilterTable(fields)
//getValuesFilterTableHTCondorJob
{
// var fields = ['fOwn', 'fWmsId', 'fGlJobId', 'fSubFrom', 'fSubTo', 'fRunT', 
//               'fSt', 'fStatus', 'fPri']
 var f = [];

 for (i in fields){
     k = fields[i];
     val = gFV(k);
     if(val.length>0){f.push({'name': k, 'value': val}); }
 }
 return f;
}



$.fn.dataTableExt.oApi.fnReloadAjax = function ( oSettings, sNewSource, fnCallback, bStandingRedraw )
{
    if ( typeof sNewSource != 'undefined' && sNewSource != null )
    {
        oSettings.sAjaxSource = sNewSource;
    }
    this.oApi._fnProcessingDisplay( oSettings, true );
    var that = this;
    var iStart = oSettings._iDisplayStart;
 
    this.fnDraw();
}
function getURLforFilter(f){
	// upURL -- update URL with filter
	// f ... filter dictionary
	var nH = "";
	for (x in f)
	{
		if (x != 0){
			nH += '&';
		}
		nH += encodeURIComponent(f[x].name) + '=' + encodeURIComponent(f[x].value) ;
	}
	return nH;
}

function upURL(f){
	// upURL -- update URL with filter
	// f ... filter dictionary
	var prefixChar = "?";
	var nH = "";
	nH = prefixChar+getURLforFilter(f);
//	console.debug('current hash   nH='+window.location.hash);
//	console.debug('current search nH='+window.location.search);
//	console.debug('future         nH='+nH);
	if ((nH.length>1) &&  (nH != window.location.search)){
		window.location.search = nH;
	}
//	console.debug('window.location.hash: '+window.location.hash);
//	console.debug('window.location.search: '+window.location.search);
}


//function getHashParams() {
function getUrlParams() {
	// getHashParams
	    var urlParams = [];
	    var e,
	        a = /\+/g,  // Regex for replacing addition symbol with a space
	        r = /([^&;=]+)=?([^&;]*)/g,
	        d = function (s) { return decodeURIComponent(s.replace(a, " ")); },
//	        q = window.location.hash.substring(1);
	        q = window.location.search.substring(1);
	    while (e = r.exec(q))
	       urlParams.push({ 'name': d(e[1]), 'value': d(e[2]) });
	    return urlParams;
}

function getFilterURL(){
	// get filter from URL to populate filter table
	    filter = getUrlParams();
	    if ( typeof filter != 'undefined' && filter != null ){
	        pgst = 'fltr';
	    } else {
	        pgst = 'ini';
	    }
}

function drawTable(stFlag){
	// nuke old table with old data
	if ( typeof oTable != 'undefined' && oTable != null ){
		oTable.fnClearTable();
	}
	// get filter parameters
	fltr=getValuesFilterTable(fields);

	//// moved URL update here   
	// update GET parameters
	upURL(fltr);

	// create new table with new data
	oTable = $("#" + tableid).dataTable( {
			"sPaginationType": "full_numbers",
			"bDestroy": true,
			"aLengthMenu": [ [300, 500, 750, 1000], [300, 500, 750, 1000] ],
			"sDom": '<"H"lfr><t><"F"ip>',
			"iDisplayLength": 300,
			"bProcessing": true,
			"bServerSide": true,
			"bFilter": false,
			"bPaginate": true,
			"sAjaxSource": datasrc,
//		"bScrollInfinite": true,
//		"sScrollY": "200px", 
			"bScrollCollapse": true,
			"sScrollX": "100%",
			"bJQueryUI": true,
			"fnServerData": function ( sSource, aoData, fnCallback ) {
				aoData.push({'name': 'csrfmiddlewaretoken', 'value': csrftoken});
				aoData.push({'name': 'pgst', 'value': stFlag});
				$.merge( aoData, fltr )
				$.ajax( {
					"dataType": 'json',
					"url": sSource,
					"data": aoData,
					"type": "POST", 
					"success": fnCallback,
					"async":true,
					"error": function (xhr, error, thrown) {
//						alert("THERE IS AN ERROR DataTable in drawTable error="+error);
						console.debug("error="+error);
						if ( error == "parsererror" ) 
							apprise( "DataTables warning: JSON data" + 
								" from server could not be parsed. " +
								"This is caused by a JSON formatting " +
								"error." 
							);
					}
				} );
			}, 
			"aoColumns": colsOrig, 
			"aoColumnDefs": [
				// produsername + workinggroup
				{
					"mRender": function ( data, type, row ) {
						var a = '<a href="'
							+ prefix 
							+ Django.url('user:useractivity')
							+ '?ProdUserName=' + data
							+ '" target="_blank">' +
							data + '</a>' +' / '+ row.workinggroup;
						return a;
						return data + ' ' + row.jobsetid + ' / ' + row.workinggroup;
					},
					"aTargets": [ fieldIndices.produsername ]
				},
				// JEDI Task ID
				{
					"mRender": function ( data, type, row ) {
					// TODO: add link to task page
//						return data;
//					+ Django.url('prodtask:task', {'rid': data})
//					+ Django.url('todoview:todoTaskDescription', {'taskid': data})
					var a = '<a href="'
					+ prefix 
					+ Django.url('prodtask:task', {'rid': data})
					+ '" target="_blank">' +
					data + '</a>' ;
				return a;
					},
					"aTargets": [ fieldIndices.jeditaskid ]
				},
				// PanDA ID
				{
					"mRender": function ( data, type, row ) {
					var a = '<a href="'
								+ prefix 
								+ Django.url('jobDetails', {'pandaid': data})
							+ '" target="_blank">' + data + '</a>'
						;
						return a;
					},
					"aTargets": [ fieldIndices.pandaid ]
				},
				// Job status
				{
					"mRender": function ( data, type, row ) {
						if (data === 'failed'){
							var a = 
								'<span style="color:red;">'
								+ data
								+ '</span>';
							return a;
						} else {
							return data;
						};
					},
					"aTargets": [ fieldIndices.jobstatus ]
				},
				// Site+Cloud
				{
					"mRender": function ( data, type, row ) {
					// TODO: add link to site activity page
						return row.cloud + '/'+ data ;
					},
					"aTargets": [ fieldIndices.computingsite ]
				},
				{ "bVisible": false, "aTargets": [ fieldIndices.cloud ] }
			]
		} );

		var smryAoData = [];
		smryAoData.push({'name': 'csrfmiddlewaretoken', 'value': csrftoken});
		smryAoData.push({'name': 'pgst', 'value': stFlag});
		$.merge( smryAoData, fltr )
		$.ajax( {
			"dataType": 'json',
			"url": datasrcsmry,
			"data": smryAoData,
			"type": "POST", 
			"success": function(data,status){
//				console.debug(data);
//				console.debug(data.aaData);
				buildSummary(tableidsmry, data);
			},
			"async":true,
			"error": function (xhr, error, thrown) {
//				alert("THERE IS AN ERROR smrydata error="+error);
				if ( error == "parsererror" ) 
					apprise( "DataTables warning: JSON data" + 
						" from server could not be parsed. " +
						"This is caused by a JSON formatting " +
						"error." 
					);
			}
		} );

//	console.debug("end of drawTable");
}

//showFilter({{tableid}}, {{caption}})
function showFilter(tableid, caption){
	$("#div-table-filter-" + tableid).show();
	$("#sh-filter-" + tableid).attr({
		src: staticurl + '/images/cross.png',
		title: "Hide filter of " + caption, 
		alt: "Hide filter of " + caption 
	});

}

// hideFilter({{tableid}}, {{caption}})
function hideFilter(tableid, caption){
	$("#div-table-filter-" + tableid).hide();
	$("#sh-filter-" + tableid).attr({
		src: staticurl + '/images/filter.png',
		title: "Show filter of " + caption, 
		alt: "Show filter of " + caption 
	});
}

function getColumnTitles()
{
    for (var i=1;i<colsOrig.length; i++ ){
        colsDict[colsOrig[i].mDataProp] = colsOrig[i].sTitle;
    }
}

function gTV(key, tD, dD, colspan, tagType){
    var ret = '<td><b>' + tD[key] + '</b></td><td';
    if(typeof(colspan) != 'undefined')
    {
        cs = colspan * 2 - 1;
        ret += ' colspan=' + cs + ' ';
    }
    ret += '>';
    if (tagType=='a'){
        ret += '<a href="' + dD[key] + '" target="_blank">'+ dD[key] + '</a>';
    } else {
        ret += dD[key];
    }
    ret += '</td>';
    return ret;
}


function fnFormatDetails(tD, dD){
//  fnFormatDetailsHTCondorJob: detail data formatting for HTCondorJob instances
//  args:
//      tD ... titleDict: dictionary with key=field name, value=title
//      dD ... dataDict:  dictionary with key=field name, value=value of the field
//  returns:
//      HTML code of table rows
  return '&nbsp;'
  ;
//+ gTV("p_description", tD, dD, 3)
}

function fnFormatDetails( oTable, nTr )
{
    var oData = oTable.fnGetData( nTr );
    if (Object.keys(colsDict).length === 0){
        getColumnTitles();
    }
    var sOut =
      '<div class="columns">'+
        '<table cellpadding="5" cellspacing="0" border="0">'
    ;
    sOut += fnFormatDetails(colsDict, oData);
    sOut += 
        '</table>'+
      '</div>';
    return sOut;
}



function drawTableListActiveUsers(){
	// nuke old table with old data
	if ( typeof oTable != 'undefined' && oTable != null ){
		oTable.fnClearTable();
	}
	// create new table with new data
	oTable = $("#" + tableid).dataTable( {
			"sPaginationType": "full_numbers",
			"bDestroy": true,
			"aLengthMenu": [ [500, 1000, 1500], [500, 1000, 1500] ],
			"sDom": '<"H"lfr><t><"F"ip>',
			"iDisplayLength": 500,
			"bProcessing": true,
			"bServerSide": true,
			"bFilter": false,
			"bPaginate": true,
			"sAjaxSource": datasrc,
			"bScrollCollapse": true,
			"sScrollX": "100%",
			"bJQueryUI": true,
			"fnServerData": function ( sSource, aoData, fnCallback ) {
				aoData.push({'name': 'csrfmiddlewaretoken', 'value': csrftoken});
//				aoData.push({'name': 'pgst', 'value': stFlag});
//				$.merge( aoData, fltr )
				$.ajax( {
					"dataType": 'json',
					"url": sSource,
					"data": aoData,
					"type": "POST", 
					"success": fnCallback,
					"async":true,
					"error": function (xhr, error, thrown) {
//						alert("THERE IS AN ERROR DataTable in drawTable error="+error);
						console.debug("error="+error);
						if ( error == "parsererror" ) 
							apprise( "DataTables warning: JSON data" + 
								" from server could not be parsed. " +
								"This is caused by a JSON formatting " +
								"error." 
							);
					}
				} );
			}, 
			"aoColumns": colsOrig, 
			"aoColumnDefs": [
				// name
				{
					"mRender": function ( data, type, row ) {
						var a = '<a href="'
							+ prefix 
							+ Django.url('user:useractivity')
							+ '?ProdUserName=' + data
							+ '" target="_blank">' +
							data + '</a>';
						return a;
//						return data;
					},
					"aTargets": [ fieldIndices.name ]
				}
			]
		} );


//	console.debug("end of drawTableListActiveUsers");
}


function drawTableUserActivity(stFlag){
	// nuke old table with old data
	if ( typeof oTable != 'undefined' && oTable != null ){
		oTable.fnClearTable();
	}
	// get filter parameters
	fltr=getValuesFilterTable(fields);

	//// moved URL update here   
	// update GET parameters
	upURL(fltr);

	// create new table with new data
	oTable = $("#" + tableid).dataTable( {
			"sPaginationType": "full_numbers",
			"bDestroy": true,
			"aLengthMenu": [ [300, 500, 750, 1000], [300, 500, 750, 1000] ],
			"sDom": '<"H"lfr><t><"F"ip>',
			"iDisplayLength": 300,
//		"iDisplayLength": 5,
			"bProcessing": true,
			"bServerSide": true,
			"bFilter": false,
			"bPaginate": true,
			"sAjaxSource": datasrc,
//		"bScrollInfinite": true,
//		"sScrollY": "200px", 
			"bScrollCollapse": true,
			"sScrollX": "100%",
			"bJQueryUI": true,
			"fnServerData": function ( sSource, aoData, fnCallback ) {
				aoData.push({'name': 'csrfmiddlewaretoken', 'value': csrftoken});
				aoData.push({'name': 'pgst', 'value': stFlag});
				$.merge( aoData, fltr )
				$.ajax( {
					"dataType": 'json',
					"url": sSource,
					"data": aoData,
					"type": "POST", 
					"success": fnCallback,
					"async":true,
					"error": function (xhr, error, thrown) {
//						alert("THERE IS AN ERROR DataTable in drawTable error="+error);
						console.debug("error="+error);
						if ( error == "parsererror" ) 
							apprise( "DataTables warning: JSON data" + 
								" from server could not be parsed. " +
								"This is caused by a JSON formatting " +
								"error." 
							);
					}
				} );
			}, 
			"aoColumns": colsOrig, 
			"aoColumnDefs": [
				// produsername + jobsetid + workinggroup
				{
					"mRender": function ( data, type, row ) {
						var a = '<a href="'
							+ prefix 
							+ Django.url('user:useractivity')
							+ '?ProdUserName=' + data 
							+ '&JobSetID=' + row.jobsetid
							+ '" target="_blank">' 
							+ data + ':' + row.jobsetid
							+ '</a>'; 
							if (row.workinggroup != ''){
								a+= ' / '+ row.workinggroup;
							}
						return a;
//						return data + ' ' + row.jobsetid + ' / ' + row.workinggroup;
					},
					"aTargets": [ fieldIndices.produsername ]
				},
				// JEDI Task ID
				{
					"mRender": function ( data, type, row ) {
					// TODO: add link to task page
//						return data;
//					+ Django.url('prodtask:task', {'rid': data})
//					+ Django.url('todoview:todoTaskDescription', {'taskid': data})
					var a = '<a href="'
					+ prefix 
					+ Django.url('prodtask:task', {'rid': data})
					+ '" target="_blank">' +
					data + '</a>' ;
				return a;
					},
					"aTargets": [ fieldIndices.jeditaskid ]
				},
				// PanDA ID
				{
					"mRender": function ( data, type, row ) {
					var a = '<a href="'
								+ prefix 
								+ Django.url('jobDetails', {'pandaid': data})
							+ '" target="_blank">' + data + '</a>'
						;
						return a;
					},
					"aTargets": [ fieldIndices.pandaid ]
				},
				// Job status
				{
					"mRender": function ( data, type, row ) {
						if (data === 'failed'){
							var a = 
								'<span style="color:red;">'
								+ data
								+ '</span>';
							return a;
						} else {
							return data;
						};
					},
					"aTargets": [ fieldIndices.jobstatus ]
				},
				// Site+Cloud
				{
					"mRender": function ( data, type, row ) {
					// TODO: add link to site activity page
						return row.cloud + '/'+ data ;
					},
					"aTargets": [ fieldIndices.computingsite ]
				},
				// Attempt
				{
					"mRender": function ( data, type, row ) {
						console.debug('att='+data);
						return data ;
					},
					"aTargets": [ fieldIndices.attempt ]
				},
				{ "bVisible": false, "aTargets": [ fieldIndices.cloud ] }
			]
		} );

		var smryAoData = [];
		smryAoData.push({'name': 'csrfmiddlewaretoken', 'value': csrftoken});
		smryAoData.push({'name': 'pgst', 'value': stFlag});
		$.merge( smryAoData, fltr )
		$.ajax( {
			"dataType": 'json',
			"url": datasrcsmry,
			"data": smryAoData,
			"type": "POST", 
			"success": function(data,status){
//				console.debug(data);
//				console.debug(data.aaData);
				buildSummary(tableidsmry, data);
			},
			"async":true,
			"error": function (xhr, error, thrown) {
//				alert("THERE IS AN ERROR smrydata error="+error);
				if ( error == "parsererror" ) 
					apprise( "DataTables warning: JSON data" + 
						" from server could not be parsed. " +
						"This is caused by a JSON formatting " +
						"error." 
					);
			}
		} );

//	console.debug("end of drawTableUserActivity");
}
