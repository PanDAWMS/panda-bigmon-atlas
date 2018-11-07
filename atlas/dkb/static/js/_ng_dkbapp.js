
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

    select_show_tasks = function(http, search_string, is_analy){
        var toSend = {search_string:search_string};
        var search_url = Django.url('dkb:search_string_to_url');
        http.post(search_url,toSend).
          success(function(data, status, headers, config) {
            var url = data.url;
            if (is_analy){
                window.location.href = '#/task_keywords_analy/'+url;
            } else {
                window.location.href = '#/task_keywords/'+url;
            }

          }).
          error(function(data, status, headers, config) {
            console.log(status);
          });
    };

    DKBApp.controller('DKBCtrl',['$scope','$http',  '$location',
        function (scope, http, location){

            scope.showTasks= function(){
                select_show_tasks(http, scope.keywords,scope.is_analy);
            }


        }]);

    dkbSummary = function(scope, http, routeParams, is_analy){
            scope.loading = true;
            scope.keywords  = routeParams.search_string;
            scope.is_analy = is_analy;
            scope.showTasks= function(){
                select_show_tasks(http, scope.keywords,scope.is_analy);
            };
            scope.manageTasks= function(){
                var taskIDs = [];
                for (i=0;i<scope.tasks.length;i++){
                    taskIDs.push(scope.tasks[i].taskid)
                }
                var toSend = {taskIDs:taskIDs};
                http.post(Django.url('dkb:tasks_from_list'), toSend).
                 success(function(data, status, headers, config) {
                     window.location.href = '/reqtask';

                 }).
                error(function(data, status, headers, config) {
                            if (data.message != undefined){
                                alert(data.message);
                            }

                });
            };
            var toSend = {search_string:routeParams.search_string};
            var result_url = Django.url('dkb:es_task_search');
            if (is_analy){
                 result_url = Django.url('dkb:es_task_search_analy');
            }
            http.post(result_url, toSend).
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
    };

    DKBApp.controller('DKBSummaryCtrl',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {
            dkbSummary(scope, http, routeParams, false);

        }]);
    DKBApp.controller('DKBSummaryCtrlAnaly',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {

         dkbSummary(scope, http, routeParams, true);
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
    var output_tasks={};
    function loadRatio(http,ami_tag,project,scope){
            output_tasks={};
            http.get(Django.url('dkb:deriv_output_proportion',project,ami_tag)).
             success(function(data, status, headers, config) {
                scope.ratio= data;
                scope.is_loading = false;
                for(var i=0;i<data.length;i++){
                    output_tasks[data[i]['output']] = data[i]['tasks_ids']
                }

             }).
            error(function(data, status, headers, config) {
                        if (data.message != undefined){
                            alert(data.message);
                        }
                         scope.is_loading = false;

            });
    }

    DKBApp.controller('DKBDerivRatio',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {
            scope.ami_tag = '';
            scope.project = '';
            scope.is_loading = false;
            if ('amitag' in routeParams){
                scope.ami_tag = routeParams.amitag;
            }
            if ('project' in routeParams){
                scope.project = routeParams.project;
            }
            if ((scope.ami_tag != '')&&(scope.project != '')){
                scope.is_loading = true;
                loadRatio(http,scope.ami_tag,scope.project,scope)
            }
            scope.showRatio = function () {
                 scope.is_loading = true;
                 var new_ami_tag = scope.ami_tag.split(/[\s,]+/).join(',');
                scope.ami_tag = new_ami_tag;
                window.location.href = '/dkb/#/deriv_ratio/?amitag='+ new_ami_tag + '&project='+scope.project;
                loadRatio(http,scope.ami_tag,scope.project,scope)
            };
            scope.showTasks = function (output) {
                    if (output in output_tasks){
                        var toSend = {taskIDs:output_tasks[output]};
                        http.post(Django.url('dkb:tasks_from_list'), toSend).
                         success(function(data, status, headers, config) {
                             window.location.href = '/reqtask';

                         }).
                        error(function(data, status, headers, config) {
                                    if (data.message != undefined){
                                        alert(data.message);
                                    }

                        });
                    }
            }
        }]);

        /**
         * Filesize Filter
         * @Param length, default is 0
         * @return string
         */
        DKBApp.filter('myfilesize', function () {
                return function (size) {
                    if (isNaN(size))
                        size = 0;

                    if (size < 1024)
                        return size + ' B';

                    size /= 1024;

                    if (size < 1024)
                        return size.toFixed(2) + ' KB';

                    size /= 1024;

                    if (size < 1024)
                        return size.toFixed(2) + ' MB';

                    size /= 1024;

                    if (size < 1024)
                        return size.toFixed(2) + ' GB';

                    size /= 1024;

                    return size.toFixed(2) + ' TB';
                };
            });

        DKBApp.controller('DKBStepStat',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {
            scope.hashtag = '';
            scope.is_loading = false;
            if ('hashtag' in routeParams){
                scope.hashtag = routeParams.hashtag;
            }

            if (scope.hashtag != ''){
                scope.is_loading = true;
                    var  toSend = scope.hashtag.toString() ;
                    console.log(toSend);
                    http.post(Django.url('dkb:step_hashtag_stat'),toSend).
                     success(function(data, status, headers, config) {
                        scope.steps= data;
                        scope.is_loading = false;

                     }).
                    error(function(data, status, headers, config) {
                                if (data.message != undefined){
                                    alert(data.message);
                                }
                                 scope.is_loading = false;

                    });
            }

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
          when('/task_keywords_analy/:search_string', {
              templateUrl: '/static/html/_ng_task_summary.html',
              controller: 'DKBSummaryCtrlAnaly'
            }).
          when('/steps_groups/', {
              templateUrl: '/static/html/_ng_task_summary.html',
              controller: 'DKBStepsCampaign'
            }).
          when('/deriv_ratio/', {
              templateUrl: '/static/html/_ng_deriv_ratio.html',
              controller: 'DKBDerivRatio',
              reloadOnSearch: false
            }).
          when('/steps_stat/', {
              templateUrl: '/static/html/_ng_step_stat.html',
              controller: 'DKBStepStat',
              reloadOnSearch: false
            }).
            otherwise({
              redirectTo: '/'
            });
    });