﻿
(function (window) {

    var iVdmConfig = (function () {
        this.GetDefaultServerIP = function () {
            var hostName = document.location.hostname
            return hostName;
        };

        this.GetDefaultServerNameForKey = function () {
            return "localhost";
        };

        this.GetPortId = function () {
            return location.port;
        };
    });

    window.VdmConfig = VdmConfig = new iVdmConfig();

})(window);
