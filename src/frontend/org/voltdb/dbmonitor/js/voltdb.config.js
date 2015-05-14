
(function (window) {

    var iVoltDBConfig = (function () {

        this.GetDefaultServerIP = function () {
            return window.location.hostname;
        };

        this.GetDefaultServerNameForKey = function () {
            return window.location.hostname;
        };

        this.GetPortId = function () {
            return window.location.port || 8080;
        };
    });

    window.VoltDBConfig = VoltDBConfig = new iVoltDBConfig();

})(window);
    