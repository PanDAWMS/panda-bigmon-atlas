
    var DKBServices = angular.module('DKBServices', ['ngResource']);
    DKBServices.run(run);
    run.$inject = ['$http'];
    /**
    * @name run
    * @desc Update xsrf $http headers to align with Django's defaults
    */
    function run($http) {
      $http.defaults.xsrfHeaderName = 'X-CSRFToken';
      $http.defaults.xsrfCookieName = 'csrftoken';
    }


    var DKBApp = angular.module('DKBApp',['ngRoute','DKBServices']);
       DKBApp.config(function($resourceProvider) {
       $resourceProvider.defaults.stripTrailingSlashes = false;
   });

    DKBApp.directive('atlastask', function() {
          return {
            scope: {
                taskMeta: '=taskmeta'

            },
            templateUrl: '/static/html/_ng_task.html',
              link: function (scope){
                  scope.showStuff =    function (obj) {
                    if(obj.show == undefined){
                        obj.show = true;
                    } else if (obj.show){
                        obj.show = false;
                    } else {
                        obj.show = true;
                    }
                };

              }
          };
        });
    var requestBaseLink =  Django.url('prodtask:input_list_approve',1).slice(0,-2);
    var taskBaseLink = Django.url('prodtask:task',1).slice(0,-2);

    DKBApp.controller('DKBCtrl',['$scope','$http',  '$location',
        function (scope, http, location){

            scope.showTasks= function(){
                var toSend = {search_string:scope.keywords};
                http.post(Django.url('dkb:search_string_to_url'),toSend).
                  success(function(data, status, headers, config) {
                    var url = data.url;
                    window.location.href = '#/task_keywords/'+url;
                  }).
                  error(function(data, status, headers, config) {
                    console.log(status);
                  });
            }


        }]);

    DKBApp.controller('DKBSummaryCtrl',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {

            scope.loading = true;
            scope.keywords  = routeParams.search_string;

            scope.showTasks= function(){
                var toSend = {search_string:scope.keywords};
                http.post(Django.url('dkb:search_string_to_url'),toSend).
                  success(function(data, status, headers, config) {
                    var url = data.url;
                    window.location.href = '#/task_keywords/'+url;
                  }).
                  error(function(data, status, headers, config) {
                    console.log(status);
                  });
            };
            var toSend = {search_string:routeParams.search_string};
            http.post(Django.url('dkb:es_task_search'), toSend).
             success(function(data, status, headers, config) {
                scope.tasks = data.tasks;
                scope.total = data.total;
                scope.loading = false;

             }).
            error(function(data, status, headers, config) {
                        if (data.message != undefined){
                            alert(data.message);
                        }

            });
            http.post(Django.url('dkb:search_string_to_url'), toSend).
             success(function(data, status, headers, config) {
                scope.query_string = data.query_string;

             }).
            error(function(data, status, headers, config) {
                        if (data.message != undefined){
                            alert(data.message);
                        }

            });
        }]);
    DKBApp.controller('DKBStepsCtrl',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {

            scope.loading = true;
            scope.query_string  = '';

            var toSend = {search_string:routeParams.search_string};
            http.post(Django.url('dkb:es_task_search'), toSend).
             success(function(data, status, headers, config) {
                scope.tasks = data;
                console.log(data);
                scope.loading = false;

             }).
            error(function(data, status, headers, config) {
                        if (data.message != undefined){
                            alert(data.message);
                        }

            });
            http.post(Django.url('dkb:search_string_to_url'), toSend).
             success(function(data, status, headers, config) {
                scope.query_string = data.query_string;

             }).
            error(function(data, status, headers, config) {
                        if (data.message != undefined){
                            alert(data.message);
                        }

            });
        }]);
    DKBApp.config(function($routeProvider) {
          $routeProvider.
            when('/', {
              templateUrl:'/static/html/_ng_task_search.html',
              controller: 'DKBCtrl as DKBModel'
            }).
          when('/task_keywords/:search_string', {
              templateUrl: '/static/html/_ng_task_summary.html',
              controller: 'DKBSummaryCtrl'
            }).
          when('/steps_groups/', {
              templateUrl: '/static/html/_ng_task_summary.html',
              controller: 'DKBStepsCampaign'
            }).
            otherwise({
              redirectTo: '/'
            });
    });