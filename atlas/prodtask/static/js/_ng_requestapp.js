    var ProductionRequestServices = angular.module('ProductionRequestServices', ['ngResource']);
    ProductionRequestServices.run(run);
    run.$inject = ['$http'];
    /**
    * @name run
    * @desc Update xsrf $http headers to align with Django's defaults
    */
    function run($http) {
      $http.defaults.xsrfHeaderName = 'X-CSRFToken';
      $http.defaults.xsrfCookieName = 'csrftoken';
    }


    var ProductionRequestApp = angular.module('ProductionRequestApp',['ngRoute','ProductionRequestServices','datatables']);
       ProductionRequestApp.config(function($resourceProvider) {
       $resourceProvider.defaults.stripTrailingSlashes = false;
   });




ProductionRequestApp.controller('WithAjaxCtrl',['DTOptionsBuilder','DTColumnBuilder','$http','$q',
function (DTOptionsBuilder, DTColumnBuilder,http, q) {
    var vm = this;
        // vm.dtOptions = DTOptionsBuilder.fromSource('/prodtask/production_request_api/')
        // .withPaginationType('full_numbers');
    vm.dtOptions = DTOptionsBuilder.newOptions()
        .withOption('ajax', {
         // Either you specify the AjaxDataProp here
         // dataSrc: 'data',
         url: '/prodtask/production_request_api/',
     })
     // or here
     .withDataProp('data')
        .withOption('processing', true)
        .withOption('serverSide', true)
        .withPaginationType('full_numbers');
    // vm.dtOptions = DTOptionsBuilder.fromFnPromise(function() {
    //     var defer = q.defer();
    //     http.get('/prodtask/production_request_api/').then(function(result) {
    //         defer.resolve(result.data);
    //     });
    //     return defer.promise;
    // }).withPaginationType('full_numbers');

    vm.dtColumns = [
        DTColumnBuilder.newColumn('reqid').withTitle('ID'),
        DTColumnBuilder.newColumn('description').withTitle('Description'),
        DTColumnBuilder.newColumn('cstatus').withTitle('Status')
    ];
}]);

ProductionRequestApp.config(function($routeProvider) {
      $routeProvider.
        when('/', {
          templateUrl:'/static/html/_ng_request_table.html',
          controller: 'WithAjaxCtrl as showCase'
        })

});