(function ($) {
    $.fn.adminOverview = function (option) {
        // set some defaults
        var defaults = {

        };

        var options = $.extend({}, defaults, option);

        //write blocks and function calls that are to be executed during load operation
        $(document).ready(function () {
            //site per host
            $("#navAdmin").on("click", function () {
                getAdminConfiguration();

            });

        });

        //inner functions
        var getAdminConfiguration = function () {
            voltDbRenderer.getAdminconfiguration(function (connection) {
                getSiteByHost(connection);

            });


        };

        var getSiteByHost = function (connection) {
            var siteCount = voltDbRenderer.getSiteCountByHost(connection);
            $('td[name=site_per_host]').next('td').text(siteCount);


        };

    };


})(jQuery);