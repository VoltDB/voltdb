
(function (window) {

    var iVoltDBConfig = (function () {

        this.GetDefaultServerIP = function () {
            return window.location.hostname;
        };

        this.GetDefaultServerNameForKey = function () {
            return window.location.hostname;
        };

        this.GetPortId = function () {
            return window.location.port;
        };
    });

    window.VoltDBConfig = VoltDBConfig = new iVoltDBConfig();

})(window);
