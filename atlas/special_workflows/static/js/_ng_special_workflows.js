
    var SpecialWorkflowsServices = angular.module('SpecialWorkflowsServices', ['ngResource']);
    SpecialWorkflowsServices.run(run);
    run.$inject = ['$http'];
    /**
    * @name run
    * @desc Update xsrf $http headers to align with Django's defaults
    */
    function run($http) {
      $http.defaults.xsrfHeaderName = 'X-CSRFToken';
      $http.defaults.xsrfCookieName = 'csrftoken';
    }


    var SpecialWorkflowsApp = angular.module('SpecialWorkflowsApp',['ngRoute','SpecialWorkflowsServices']);
       SpecialWorkflowsApp.config(function($resourceProvider) {
       $resourceProvider.defaults.stripTrailingSlashes = false;
   });




    SpecialWorkflowsApp.controller('SpecialWorkflowsCtrl',['$scope','$http',
        function (scope, http){
                scope.test = 'Special workflows';

        }]);

        SpecialWorkflowsApp.controller('SpecialWorkflowsIDDSCtrl',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {
                scope.saveStep = () => {
                var sendData = {'outputPostProcessing':scope.outputPostProcessing,'template_input':scope.template_input,
                'terminations': scope.terminations};
                http.post(construct_django_url('/special_workflows/idds_postproc_save/',scope.step.step_id),sendData).
                    success(function(data, status, headers, config) {
                            alert('Step is saved');

                         }).
                        error(function(data, status, headers, config) {
                            console.log(data);
                            alert('Problem during save saving');

                        });

            };
            scope.changePattern = () => {
              scope.outputPostProcessing = scope.patterns[scope.choosenPattern].outputPostProcessing;
              scope.template_input = scope.patterns[scope.choosenPattern].template_input;
              scope.terminations = scope.patterns[scope.choosenPattern].terminations;
            };
            scope.addTermination = () => {
              scope.terminations.push({'comparison':'gt','value':0});
            };
            scope.request_id =routeParams.request_id;
            scope.outputPostProcessing = {data:{}};
            scope.patterns = [{'name':'original'}];
            http.get(construct_django_url('/special_workflows/idds_postproc/',scope.request_id)).
                         success(function(data, status, headers, config) {
                             scope.step = data.step;
                             scope.outputPostProcessing = data.outputPostProcessing;
                             scope.template_input = data.template_input;
                             scope.terminations = data.terminations;
                             scope.patterns[0]['outputPostProcessing'] =  data.outputPostProcessing;
                             scope.patterns[0]['template_input'] =  data.template_input;
                             scope.patterns[0]['terminations'] =  data.terminations;

                             if (scope.step.submitted){
                                 http.get(construct_django_url('/special_workflows/idds_tasks/',scope.request_id)).
                                     success(function(data, status, headers, config) {
                                        scope.tasks = data.tasks;
                                     }).
                                    error(function(data, status, headers, config) {
                                        console.log(data)

                                    });
                             } else {
                                 http.get(construct_django_url('/special_workflows/idds_get_patterns/')).
                                     success(function(data, status, headers, config) {
                                            for(var i=0;i<data.patterns.length;i++){
                                                scope.patterns[i+1] = data.patterns[i];
                                            }
                                     }).
                                    error(function(data, status, headers, config) {
                                        console.log(data)

                                    });
                             }
                         }).
                        error(function(data, status, headers, config) {
                            console.log(data)

                        });

            }


          ]);

    SpecialWorkflowsApp.config(function($routeProvider) {
          $routeProvider.
            when('/', {
              templateUrl:'/static/html/_ng_special_workflows_main.html',
              controller: 'SpecialWorkflowsCtrl',

            }).when('/idds/:request_id', {
              templateUrl: '/static/html/_ng_idds_workflows.html',
              controller: 'SpecialWorkflowsIDDSCtrl',
            })

    });