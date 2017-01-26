/* bigpandamon.js
 * 
 */
var selectedProfile='violet';

//--------------
function setProfile(){
//    getProfileHash();
	switch(selectedProfile)
	{
	case 'violet':
	  setProfileViolet();
	  break;
	case 'blue':
	  setProfileBlue();
	  break;
	case 'red':
	  setProfileRed();
	  break;
	default:
	  setProfileViolet();
	}
}
//--------------
function setProfileHash(){
  window.location.hash = selectedProfile;
}
//--------------
function getProfileHash(){
	switch(window.location.hash)
	{
	case 'violet':
	  selectedProfile = 'violet'; 
	  break;
	case 'blue':
	  selectedProfile = 'blue'; 
	  break;
	case 'red':
	  selectedProfile = 'red'; 
	  break;
	default:
	  selectedProfile = 'violet'; 
	}
}
//--------------
function changeProfile(){
    $("#el-box-violet").click(function(){setProfileViolet();});
    $("#el-box-blue").click(function(){setProfileBlue();});
    $("#el-box-red").click(function(){setProfileRed();});
}
//--------------
function setProfileViolet(){
   selectedProfile='violet';
   var colorBg = "#C281B2";
   var colorA = "#803253";
   var colorAmenu = "#000000";
    $( "a" ).css("color", colorA);
    $( "#lh-col-top1" ).css("background-color", colorBg);
    $( "#hdr-nav" ).css("background-color", colorBg);
	    $( "#hdr-nav a" ).css("color", colorAmenu);
    $( "#hdr-nav-menu" ).css("background-color", colorBg);
    	$( "#hdr-nav-menu a" ).css("color", colorAmenu);
    $( "#hdr-nav-chain" ).css("background-color", colorBg);
	    $( "#hdr-nav-chain a" ).css("color", colorAmenu);
    $( "#hdr-title" ).css("border-left-color", colorBg);
    $( "#c-block" ).css("border-top-color", colorBg);
    $( "#el-box-violet" ).html("&#10004;");
    $( "#el-box-blue" ).html("");
    $( "#el-box-red" ).html("");
//   setProfileHash();
}
//--------------
function setProfileBlue(){
   selectedProfile='blue';
   var colorBg = "#2795B6";
   var colorA = "#2795B6";
   var colorAmenu = "#000000";
    $( "a" ).css("color", colorA);
    $( "#lh-col-top1" ).css("background-color", colorBg);
    $( "#hdr-nav" ).css("background-color", colorBg);
	    $( "#hdr-nav a" ).css("color", colorAmenu);
    $( "#hdr-nav-menu" ).css("background-color", colorBg);
    	$( "#hdr-nav-menu a" ).css("color", colorAmenu);
    $( "#hdr-nav-chain" ).css("background-color", colorBg);
	    $( "#hdr-nav-chain a" ).css("color", colorAmenu);
    $( "#hdr-title" ).css("border-left-color", colorBg);
    $( "#c-block" ).css("border-top-color", colorBg);
    $( "#el-box-violet" ).html("");
    $( "#el-box-blue" ).html("&#10004;");
    $( "#el-box-red" ).html("");
//   setProfileHash();
}
//--------------
function setProfileRed(){
   selectedProfile='red';
   var colorBg = "#FF0000";
   var colorA = "#FF0000";
   var colorAmenu = "#000000";
    $( "a" ).css("color", colorA);
    $( "#lh-col-top1" ).css("background-color", colorBg);
    $( "#hdr-nav" ).css("background-color", colorBg);
	    $( "#hdr-nav a" ).css("color", colorAmenu);
    $( "#hdr-nav-menu" ).css("background-color", colorBg);
    	$( "#hdr-nav-menu a" ).css("color", colorAmenu);
    $( "#hdr-nav-chain" ).css("background-color", colorBg);
	    $( "#hdr-nav-chain a" ).css("color", colorAmenu);
    $( "#hdr-title" ).css("border-left-color", colorBg);
    $( "#c-block" ).css("border-top-color", colorBg);
    $( "#el-box-violet" ).html("");
    $( "#el-box-blue" ).html("");
    $( "#el-box-red" ).html("&#10004;");
//   setProfileHash();
}
//--------------
//--------------
//--------------


