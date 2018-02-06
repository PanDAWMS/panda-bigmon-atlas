    var globalShareServices = angular.module('globalShareServices', ['ngResource']);
    globalShareServices.run(run);

    run.$inject = ['$http'];
/**
* @name run
* @desc Update xsrf $http headers to align with Django's defaults
*/
    function run($http) {
      $http.defaults.xsrfHeaderName = 'X-CSRFToken';
      $http.defaults.xsrfCookieName = 'csrftoken';
    }


    var globalShareApp = angular.module('globalShareApp',['ngRoute','globalShareServices']);
    globalShareApp.config(function($resourceProvider) {
        $resourceProvider.defaults.stripTrailingSlashes = false;
    });

    globalShareApp.controller('globalShareCtrl',['$scope','$http', function (scope, http){

        scope.globalShareTable = [];

        var changeTree = function(parents){
            var parents_value = {};
            var total_brunch = {};
            var new_parents = [];
            if(parents.indexOf('root')>-1){
                    parents_value['root'] = 100

            }
            while(parents.length>0){
                new_parents = [];
                for(var i=0;i<parents.length;i++){
                    total_brunch[parents[i]] = 0;
                }
                for(i=0;i< scope.globalShareTable.length;i++){
                for(j=0;j<scope.globalShareTable[i].length;j++){
                    if(scope.globalShareTable[i][j]['show']){
                        if(parents.indexOf(scope.globalShareTable[i][j]['name'])>-1){
                            parents_value[scope.globalShareTable[i][j]['name']] = scope.globalShareTable[i][j]['percentage'];
                        }
                        if(parents.indexOf(scope.globalShareTable[i][j]['parent'])>-1){
                            total_brunch[scope.globalShareTable[i][j]['parent']] += scope.globalShareTable[i][j]['value'];
                            new_parents.push(scope.globalShareTable[i][j]['name']);
                        }
                    }

                }
                }
                if (new_parents.length>0){
                        for(i=0;i< scope.globalShareTable.length;i++){
                        for(j=0;j<scope.globalShareTable[i].length;j++){
                            if(scope.globalShareTable[i][j]['show']) {
                                if (parents.indexOf(scope.globalShareTable[i][j]['parent']) > -1) {
                                    scope.globalShareTable[i][j]['percentage'] = (scope.globalShareTable[i][j]['value'] / total_brunch[scope.globalShareTable[i][j]['parent']]) * parents_value[scope.globalShareTable[i][j]['parent']];
                                    if (total_brunch[scope.globalShareTable[i][j]['parent']] != 100) {
                                        scope.globalShareTable[i][j]['percentType'] = 'wrong';
                                        scope.globalShareTable[i][j]['showTotal'] = true;
                                        scope.globalShareTable[i][j]['totalGroup'] = total_brunch[scope.globalShareTable[i][j]['parent']];
                                    } else if (scope.globalShareTable[i][j]['value'] != scope.globalShareTable[i][j]['original_value']) {
                                        scope.globalShareTable[i][j]['percentType'] = 'changed';
                                        scope.globalShareTable[i][j]['showTotal'] = false;
                                    } else {
                                        scope.globalShareTable[i][j]['percentType'] = 'default';
                                        scope.globalShareTable[i][j]['showTotal'] = false;
                                    }
                                }
                            }
                        }
                    }
                }
                parents = new_parents;
            }
        };

        http.get(Django.url('gdpconfig:global_share_tree')).
              success(function(data, status, headers, config) {
                scope.globalShareTable = data;
                changeTree(['root']);
                scope.loading = false;
              }).
              error(function(data, status, headers, config) {
                 scope.loading = false;
                 alert(data);
              });
        scope.save_gs = function(){
                var toSave = {};
                var wrongSubSum = {};
                var wrongNumber = [];
                var oldValues = {};
                var hasChanges = false;
                for(i=0;i< scope.globalShareTable.length;i++){
                    for(j=0;j<scope.globalShareTable[i].length;j++){
                        if(scope.globalShareTable[i][j]['show']) {
                            if (scope.globalShareTable[i][j]['value'] != scope.globalShareTable[i][j]['original_value']) {
                                if ((scope.globalShareTable[i][j]['value'] >= 0) && (scope.globalShareTable[i][j]['value'] <= 100) && (!isNaN(scope.globalShareTable[i][j]['value']))) {
                                    toSave[scope.globalShareTable[i][j]['name']] = scope.globalShareTable[i][j]['value'];
                                    oldValues[scope.globalShareTable[i][j]['name']] = scope.globalShareTable[i][j]['original_value'];
                                    hasChanges = true;
                                } else {
                                    wrongNumber.push(scope.globalShareTable[i][j]['name']);
                                }

                            }
                            if (scope.globalShareTable[i][j]['percentType'] == 'wrong') {
                                if (scope.globalShareTable[i][j]['value'] != scope.globalShareTable[i][j]['original_value']) {
                                    wrongSubSum[scope.globalShareTable[i][j]['name']] = [scope.globalShareTable[i][j]['value'], scope.globalShareTable[i][j]['original_value']];
                                }
                            }
                        }
                    }
                }
                var errorMessage = '';
                var isError = false;
                if(wrongNumber.length>0){
                    errorMessage += 'Next shares have the wrong value: '+ wrongNumber.toString();
                    isError = true;
                }
                if((Object.keys(wrongSubSum).length != 0)&&(!isError)){
                    errorMessage += 'Next shares have been modified but the group has wrong sum(!=100):\n'+
                            'share - new value - old value\n';
                    for(var share in wrongSubSum){
                        errorMessage += share + ' - ' + wrongSubSum[share][0] + ' - ' + wrongSubSum[share][1] +'\n';
                    }
                    isError = true;
                }

                if(isError){
                    alert(errorMessage);
                }
                if ((hasChanges)&&(!isError)){
                    var confirm_message='Do you want to change shares: \n'+
                            'share - new value - old value\n';
                    for(var key in toSave){
                        confirm_message += key+' - ' + toSave[key] + ' - ' + oldValues[key]+';\n';
                    }
                    if(confirm(confirm_message)){
                        http.post(Django.url('gdpconfig:global_share_change'),toSave).
                                  success(function(data, status, headers, config) {
                                    var result = data.result;
                                    if(result != "OK"){
                                        alert(data.exception);
                                    } else {
                                        scope.alreadyObsoleted = true;
                                    }
                                  }).
                                  error(function(data, status, headers, config) {
                                    alert(data);
                                  });
                    }
                }
        };
        scope.change_value = function(parent){
            changeTree([parent])
        };

    }]);




    globalShareApp.config(function($routeProvider) {
          $routeProvider.
            when('/', {
              templateUrl:'/static/html/_ng_global_share_table.html',
              controller: 'globalShareCtrl'
            })


    });