var hashtagServices = angular.module('hashtagServices', ['ngResource']);
hashtagServices.run(run);

run.$inject = ['$http'];
/**
* @name run
* @desc Update xsrf $http headers to align with Django's defaults
*/
function run($http) {
  $http.defaults.xsrfHeaderName = 'X-CSRFToken';
  $http.defaults.xsrfCookieName = 'csrftoken';
}
hashtagServices.factory('hashtagLists', ['$resource',
  function($resource){
    return $resource(Django.url('prodtask:hashtagslists'), {}, {
      query: {method:'GET',  isArray:true},
        save: {method:'POST'}
    });
  }]);



var hashtagFormula = '';

var hashtagApp = angular.module('hashtagApp',['ngRoute','hashtagServices']);
hashtagApp.config(function($resourceProvider) {
    $resourceProvider.defaults.stripTrailingSlashes = false;
});
hashtagApp.controller('HashTagCtrl',['$scope','$http','hashtagLists', function (scope, http, hashtagLists){
    if (typeof scope.hashTags == "undefined") {
        scope.hashTags = hashtagLists.query();
        scope.hashtag_formula = '';
        var hashtagOperators = ['and_selected', 'or_selected', 'not_selected'];
        var hashtagSign = ['&', '|', '!'];
        for (var j = 0; j < scope.hashTags.length; j++) {
            for (var i = 0; i < hashtagOperators.length; i++) {
                scope.hashTags[j][hashtagOperators[i]] = false;
            }
        }
        var hashtag_formula_entyties = {};
        for (var i = 0; i < hashtagOperators.length; i++) {
            hashtag_formula_entyties[hashtagOperators[i]] = [];
        }
    }
    scope.change_hashtag_formula = function(hashtag, is_selected, operator, hashtagIndex){
            var index = 0;
            if(is_selected){
                for(var i=0; i<hashtagOperators.length;i++){
                    if(hashtagOperators[i]!=operator){
                        scope.hashTags[hashtagIndex][hashtagOperators[i]] = false;
                        index = hashtag_formula_entyties[hashtagOperators[i]].indexOf(hashtag);
                        if (index > -1) {
                            hashtag_formula_entyties[hashtagOperators[i]].splice(index, 1);
                        }
                    }
                }
                console.log(hashtag, hashtag_formula_entyties, operator);
                index = hashtag_formula_entyties[operator].indexOf(hashtag);
                if (index == -1) {
                    hashtag_formula_entyties[operator].push(hashtag);
                }


            } else {
                index = hashtag_formula_entyties[operator].indexOf(hashtag);
                if (index > -1) {
                    hashtag_formula_entyties[operator].splice(index, 1);
                }
            }
            scope.hashtag_formula = '';
            for(var i=0; i<hashtagOperators.length;i++){
                for(var j=0; j<hashtag_formula_entyties[hashtagOperators[i]].length;j++){
                    scope.hashtag_formula += hashtagSign[i] + hashtag_formula_entyties[hashtagOperators[i]][j];
                }
            }
            hashtagFormula = scope.hashtag_formula;
            console.log(scope.hashtag_formula);
    };


    scope.showTasks= function(){


            window.location.href = Django.url('reqtask:tasks_hashtags',scope.hashtag_formula);

    };
    scope.showStatistic= function(){
        var tasks = [];
        http.post(Django.url('prodtask:tasks_hashtag'),scope.hashtag_formula).
          success(function(data, status, headers, config) {
            tasks = data;
            http.post(Django.url('prodtask:tasks_statistic_steps'),tasks).
              success(function(data, status, headers, config) {
                 console.log(data);
                }).
              error(function(data, status, headers, config) {
                console.log(status);
              });
          }).
          error(function(data, status, headers, config) {
            console.log(status);
          });
    }

}]);


hashtagApp.controller('progressStatHashtagCtrl',['$scope','$http','$routeParams',

    function (scope, http, routeParams) {
        var requestProgressData = this;
        requestProgressData.steps = [];
        scope.loading = true;
        scope.showOnlyRunning = false;
        scope.search = {};
        var tasks = [];
        var hashtagFormula = routeParams.hashtags;
        console.log(routeParams.hashtags);
        http.post(Django.url('prodtask:tasks_hashtag'),hashtagFormula).
          success(function(data, status, headers, config) {
            tasks = data;console.log(data);
            http.post(Django.url('prodtask:tasks_statistic_steps'),tasks).
              success(function(data, status, headers, config) {
                 console.log(data);
                        requestProgressData.steps = data.load.step_statistic;
                        requestProgressData.chains = data.load.chains;
                        requestProgressData.hashtags = hashtagFormula;
                        scope.loading = false;
                }).
              error(function(data, status, headers, config) {
            console.log(status);
             scope.loading = false;
              });
          }).
          error(function(data, status, headers, config) {
            console.log(status);
            scope.loading = false;
          });
        scope.changeShowOnlyRunning = function () {
            console.log('rabotaet');
            if (scope.showOnlyRunning){
                scope.search.chain_status = 'running';
            } else {
                scope.search.chain_status = '';
            }
        }

    }]);



function extractTaskID(originalText){
    var parsedText = originalText.replace(/(\r\n|\n|\r|\s|;)/gm,",").split(",");
    var resultArray = [];
    for(var i= 0;i<parsedText.length;i++){
        var token=parsedText[i];
        if(token != ""){
            if(token.indexOf("tid")>-1){
                token = token.substring(token.indexOf("tid")+3,token.lastIndexOf('_'));
            }

            if(/^\d+$/.test(token)){
                resultArray.push(token);
            }
        }
    }
    return resultArray;
}

hashtagApp.controller('setTasksHashtagCtrl',['$scope','$http','$routeParams',

    function (scope, http, routeParams) {



        scope.set_hahtag = function(){
            var tasks = extractTaskID(scope.tasks_text);
            message = "Set " + scope.hashtag + " for " + tasks.length.toString() + " tasks.";
            if(confirm(message)){
                var sendData = {hashtag:scope.hashtag,tasks:tasks};
                http.post(Django.url('prodtask:set_hashtag_for_tasks'),sendData).
                  success(function(data, status, headers, config) {
                        window.location.href = '/reqtask/hashtags/&'+scope.hashtag;

                    }).
                  error(function(data, status, headers, config) {
                        alert('Error: '+ data.error);
                  });
            }

        }

    }]);



hashtagApp.config(function($routeProvider) {
      $routeProvider.
        when('/', {
          templateUrl:'/static/html/_ng_hashtags_list.html',
          controller: 'HashTagCtrl'
        }).
        when('/set_tasks/', {
          templateUrl:'/static/html/_ng_set_tasks.html',
          controller: 'setTasksHashtagCtrl'
        }).
      when('/hashtags/:hashtags', {
          templateUrl: '/static/html/_ng_request_progress.html',
          controller: 'progressStatHashtagCtrl as requestProgressData'
        })

});