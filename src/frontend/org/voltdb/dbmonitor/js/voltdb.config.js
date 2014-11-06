
(function (window) {

    var iVoltDBConfig = (function () {

        this.GetDefaultServerIP = function() {
            return "localhost";
        };

        this.GetDefaultServerNameForKey = function () {

            return "localhost";
        };

        this.GetPortId = function () {
            return window.location.port;
        };
    });

    window.VoltDBConfig = VoltDBConfig = new iVoltDBConfig();

})(window);
