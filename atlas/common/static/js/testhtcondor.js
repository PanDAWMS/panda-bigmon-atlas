
$(document).ready(function() {
    $("#example").dataTable( {
        "sPaginationType"   : "full_numbers",
        "aLengthMenu"       : [ [10, 50, 100, 500, -1], [10, 50, 100, 500, "All"] ],
        "sDom"              : '<"H"lfr><t><"F"ip>',
        "iDisplayLength"    : 10,
        "bProcessing": true,
        "bServerSide": true,
        "sAjaxSource": "/api/datatables/htcondor/list/",
        "bScrollCollapse"   : true,
        "sScrollX"          : "100%",
        "bJQueryUI"         : true,
		"fnServerData": function ( sSource, aoData, fnCallback ) {
              $.ajax( {
                "dataType": 'json',
//                "type": "POST",
                "url": sSource,
                "data": aoData,
                "success": fnCallback,
				"async":true,
				"error": function (xhr, error, thrown) {
					alert("THERE IS AN ERROR");
					if ( error == "parsererror" ) 
						apprise( "DataTables warning: JSON data" + 
							" from server could not be parsed. " +
							"This is caused by a JSON formatting " +
							"error." 
						);
					}
              } );
            }, 
        "aoColumns":  [
			{"sTitle": "wmsid"}, 
			{"sTitle": "globaljobid"}, 
			{"sTitle": "condorid"}, 
			{"sTitle": "owner"}, 
			{"sTitle": "submitted"}, 
			{"sTitle": "run_time"}, 
			{"sTitle": "st"}, 
			{"sTitle": "pri"}, 
			{"sTitle": "size"}, 
			{"sTitle": "cmd"}, 
			{"sTitle": "host"}, 
			{"sTitle": "status"}, 
			{"sTitle": "manager"}, 
			{"sTitle": "executable"}, 
			{"sTitle": "goodput"}, 
			{"sTitle": "cpu_util"}, 
			{"sTitle": "mbps"}, 
			{"sTitle": "read_"}, 
			{"sTitle": "write_"}, 
			{"sTitle": "seek"}, 
			{"sTitle": "xput"}, 
			{"sTitle": "bufsize"}, 
			{"sTitle": "blocksize"}, 
			{"sTitle": "cpu_time"}, 
			{"sTitle": "p_start_time"}, 
			{"sTitle": "p_end_time"}, 
			{"sTitle": "p_modif_time"}, 
			{"sTitle": "p_factory"}, 
			{"sTitle": "p_schedd"}, 
			{"sTitle": "p_description"}, 
			{"sTitle": "p_stdout"}, 
			{"sTitle": "p_stderr"}
		],
    } );
} );

