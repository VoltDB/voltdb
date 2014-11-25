(function ($) {
    var adminOverviewFunctions = function (options) {
        this.options = options;
        
        //inner functions
        this.getAdminConfiguration = function(connection) {            
                var adminConfigValues = voltDbRenderer.getAdminConfigurationItems(connection);
                configureAdminValues(adminConfigValues);

        };

        var configureAdminValues = function (adminConfigValues) {
            options.siteNumberHeader.text(adminConfigValues.sitesperhost);
            options.kSafety.text(adminConfigValues.kSafety);
            options.partitionDetection.className = adminConfigValues.partitionDetection == 'true' ? 'onIcon' : 'offIcon';
            
        };
        
    };

    $.fn.adminOverview = function (option) {
        // set defaults
        var adminDOMObjects = {
            siteNumberHeader: $("#site_per_host"),
            kSafety: $("#kSafety"),
            partitionDetection: $("#partitionDetection")
        };

        var options = $.extend({}, adminDOMObjects, option);

        //write blocks and function calls that are to be executed during load operation
        $(document).ready(function () {
            //site per host
            //$("#navAdmin").on("click", function () {
            //    getAdminConfiguration();

            //});

        });
        return new adminOverviewFunctions(options);
        
    };


})(jQuery);