
    var MCPatternServices = angular.module('MCPatternServices', ['ngResource']);
    MCPatternServices.run(run);
    run.$inject = ['$http'];
    /**
    * @name run
    * @desc Update xsrf $http headers to align with Django's defaults
    */
    function run($http) {
      $http.defaults.xsrfHeaderName = 'X-CSRFToken';
      $http.defaults.xsrfCookieName = 'csrftoken';
    }


    var MCPatternApp = angular.module('MCPatternApp',['ngRoute','MCPatternServices']);
       MCPatternApp.config(function($resourceProvider) {
       $resourceProvider.defaults.stripTrailingSlashes = false;
   });




    MCPatternApp.controller('MCPatternCtrl',['$scope','$http','sourcePatternUrl',
        function (scope, http,sourcePatternUrl){
            http.get(sourcePatternUrl).
              success(function(data, status, headers, config) {
                scope.patterns = data;

              }).
              error(function(data, status, headers, config) {
                console.log(status);
              });


        }]);

    MCPatternApp.controller('MCPatternUpdate',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {

            scope.showStep = function(obj) {
                    if(obj.show === undefined){
                        obj.show = true;
                    } else obj.show = !obj.show;
                };
            scope.changeStepValue = function(obj,param){
              if(scope.origin_steps[obj.id][param] !== obj[param]){
                  obj.style[param]={backgroundColor:'lightgreen'};
              }  else {
                  obj.style[param]={backgroundColor:null};
              }
            };
            scope.savePattern = function(){
                var sendData = {'steps':scope.steps,'pattern_in_use':scope.pattern_in_use,'pattern_name':scope.pattern_name};
                http.post(construct_django_url('/request_pattern/slice_pattern_save_steps/',scope.pattern_id),sendData).
                    success(function(data, status, headers, config) {
                             scope.origin_steps = {};
                             for(var i=0;i<scope.steps.length;i++){
                                 scope.steps[i].style = {};
                                 Object.keys(scope.steps[i]).forEach((key,index) =>{scope.steps[i].style[key]=''} );
                                 scope.origin_steps[scope.steps[i].id] = Object.assign({},scope.steps[i]);
                             }
                            alert('Pattern is saved');

                         }).
                        error(function(data, status, headers, config) {
                            console.log(data);
                            alert('Problem during pattern creation');

                        });

            };
            scope.pattern_id =routeParams.id;
                        http.get(construct_django_url('/request_pattern/slice_pattern_steps/',scope.pattern_id)).
                         success(function(data, status, headers, config) {
                             var steps=data.steps;
                             scope.origin_steps = {};
                             scope.amitagStyle='';
                             scope.pattern_name = data.pattern_name;
                             scope.pattern_in_use = data.pattern_in_use;
                             for(var i=0;i<steps.length;i++){
                                 if('' !== steps[i].tag) {
                                     steps[i].show = true;
                                 }
                                 steps[i].style = {};
                                 Object.keys(steps[i]).forEach((key,index) =>{steps[i].style[key]=''} );


                                 scope.origin_steps[steps[i].id] = Object.assign({},steps[i]);

                             }
                             scope.steps = steps;

                         }).
                        error(function(data, status, headers, config) {
                            console.log(data)

                        });
            }
          ]);

        MCPatternApp.controller('MCPatternClone',['$scope','$http','$routeParams',

        function (scope, http, routeParams) {

            scope.clonePattern = function(obj) {
                        var sendData= {slice:scope.pattern_id,new_name:scope.new_pattern};
                        http.post('/request_pattern/clone_pattern/',sendData).
                         success(function(data, status, headers, config) {
                             window.location.href = '#/pattern/'+data.new_pattern;
                         }).
                        error(function(data, status, headers, config) {
                            console.log(data)

                        });
                };
            scope.pattern_id =routeParams.id;
                        http.get(construct_django_url('/request_pattern/slice_pattern/',scope.pattern_id)).
                         success(function(data, status, headers, config) {
                             scope.origin_pattern = data;

                         }).
                        error(function(data, status, headers, config) {
                            console.log(data)

                        });
            }
          ]);

    MCPatternApp.config(function($routeProvider) {
          $routeProvider.
            when('/', {
              templateUrl:'/static/html/_ng_mc_patterns.html',
              controller: 'MCPatternCtrl as MCPatternModel',
              resolve: {sourcePatternUrl: function () {
                      return "/request_pattern/pattern_list/";
                  }}
            }).when('/all/', {
              templateUrl:'/static/html/_ng_mc_patterns.html',
              controller: 'MCPatternCtrl as MCPatternModel',
              resolve: {sourcePatternUrl:  function () {
                      return "/request_pattern/pattern_list_with_obsolete/";
                  }}
            }).when('/pattern/:id', {
              templateUrl: '/static/html/_ng_mc_update_pattern.html',
              controller: 'MCPatternUpdate',
            }).when('/clone/:id', {
              templateUrl: '/static/html/_ng_clone_pattern.html',
              controller: 'MCPatternClone',
            })

    });