
(function (window) {

    var iVoltDBConfig = (function () {

        //var CONFIG_MODE = "release"; //Enable this for production release.
        var CONFIG_MODE = "debug"; //Enable this for debug mode.

        //This JSON object should only used for debug purpose.
        this.PrivateToPublicIP = {
            '10.140.25.211': '184.73.30.156',
            '10.110.218.156': 'ec2-54-83-134-161.compute-1.amazonaws.com'
        };

        this.GetDefaultServerIP = function () {
            if (CONFIG_MODE == "debug") {
               // return "192.168.0.213"; //LOCAL
                 return "10.10.1.52"; //UAT
                //return "192.168.0.239";
                //return "10.10.1.52";
                //return "192.168.0.212";
                //return "184.73.30.156";
            }

            return "localhost";
        };

        this.GetDefaultServerNameForKey = function () {
            if (CONFIG_MODE == "debug")
                return "184_73_30_156";

            return "localhost";
        };

        this.GetPortId = function () {
            if (CONFIG_MODE == "debug")
                return null;

            return window.location.port;
        };
    });

    window.VoltDBConfig = VoltDBConfig = new iVoltDBConfig();

})(window);