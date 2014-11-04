﻿
(function(window) {

    var IMonitorGraphUI = (function () {

        var currentView = "seconds";
        var cpuSecCount = 0;
        var cpuMinCount = 0;
        var tpsSecCount = 0;
        var tpsMinCount = 0;
        var memSecCount = 0;
        var memMinCount = 0;
        var latSecCount = 0;
        var latMinCount = 0;
        var cpuChart;
        var ramChart;
        var latencyChart;
        var transactionChart;
        this.Monitors = {};
        this.ChartCpu = nv.models.lineChart();
        this.ChartRam = nv.models.lineChart();
        this.ChartLatency = nv.models.lineChart();
        this.ChartTransactions = nv.models.lineChart();
        
        function getEmptyData() {
            var arr = [];
            var theDate = new Date();

            for (var i = 121; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y:null };
                theDate.setSeconds(theDate.getSeconds() - 5);
            }

            return arr;
        }
        
        function getEmptyDataForMinutes() {
            var arr = [];
            var theDate = new Date();

            for (var i = 121; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y: null };
                theDate.setSeconds(theDate.getSeconds() - 30);
            }

            return arr;
        }
        
        function getEmptyDataForDays() {
            var arr = [];
            var theDate = new Date();

            for (var i = 180; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y: null };
                theDate.setMinutes(theDate.getMinutes() - 10);
            }

            return arr;
        }
        
        var dataCpu = [{
            "key": "CPU",
            "values": getEmptyData(),
            "color": "rgb(164, 136, 5)",
        }];

        var dataRam = [{
            "key": "RAM",
            "values": getEmptyData(),
            "color": "rgb(164, 136, 5)"
        }];

        var dataLatency = [{
            "key": "Latency",
            "values": getEmptyData(),
            "color": "rgb(27, 135, 200)"
        }];

        var dataTransactions = [{
            "key": "Transactions",
            "values": getEmptyData(),
            "color": "rgb(27, 135, 200)"
        }];

        nv.addGraph(function () {

            //Formats: http://www.d3noob.org/2012/12/formatting-date-time-on-d3js-graph.html
            //%b %d : Feb 01, %x : 02/01/2012, %X: HH:MM:ss
            MonitorGraphUI.ChartCpu.xAxis
                .tickFormat(function (d) {
                    return d3.time.format('%X')(new Date(d));
                });

            MonitorGraphUI.ChartCpu.xAxis.rotateLabels(-20);

            MonitorGraphUI.ChartCpu.yAxis
                .tickFormat(d3.format(',.2f'));

            MonitorGraphUI.ChartCpu.yAxis
                .axisLabel('(%)')
                .axisLabelDistance(10);

            MonitorGraphUI.ChartCpu.tooltipContent(function (key, y, e, graph) {
                return '<h3> CPU </h3>'
                    + '<p>' + e + '% at ' + y + '</p>';
            });


            MonitorGraphUI.ChartCpu.margin({ left: 80 });

            d3.select('#visualisationCpu')
                .datum(dataCpu)
                .transition().duration(500)
                .call(MonitorGraphUI.ChartCpu);

            nv.utils.windowResize(MonitorGraphUI.ChartCpu.update);

            return MonitorGraphUI.ChartCpu;
        });

        nv.addGraph(function () {
            MonitorGraphUI.ChartRam.xAxis
                .tickFormat(function (d) {
                    return d3.time.format('%X')(new Date(d));
                });
            
            MonitorGraphUI.ChartRam.xAxis.rotateLabels(-20);

            MonitorGraphUI.ChartRam.yAxis
                .tickFormat(d3.format(',.4f'));

            MonitorGraphUI.ChartRam.yAxis
                .axisLabel('(GB)')
                .axisLabelDistance(10);
            
            MonitorGraphUI.ChartRam.margin({ left: 80 });
            
            MonitorGraphUI.ChartRam.tooltipContent(function (key, y, e, graph) {
                return '<h3> RAM </h3>'
                    + '<p>' + e + ' GB at ' + y + '</p>';
            });
            
            d3.select('#visualisationRam')
                .datum(dataRam)
                .transition().duration(500)
                .call(MonitorGraphUI.ChartRam);

            nv.utils.windowResize(MonitorGraphUI.ChartRam.update);

            return MonitorGraphUI.ChartRam;
        });

        nv.addGraph(function () {

            MonitorGraphUI.ChartLatency.xAxis
                .tickFormat(function (d) {
                    return d3.time.format('%X')(new Date(d));
                });

            MonitorGraphUI.ChartLatency.xAxis.rotateLabels(-20);
            
            MonitorGraphUI.ChartLatency.yAxis
                .tickFormat(d3.format(',.2f'));

            MonitorGraphUI.ChartLatency.yAxis
                .axisLabel('(ms)')
                .axisLabelDistance(10);

            MonitorGraphUI.ChartLatency.margin({ left: 80 });
            
            MonitorGraphUI.ChartLatency.tooltipContent(function (key, y, e, graph) {
                return '<h3> Latency </h3>'
                    + '<p>' + e + ' ms at ' + y + '</p>';
            });

            d3.select('#visualisationLatency')
                .datum(dataLatency)
                .transition().duration(500)
                .call(MonitorGraphUI.ChartLatency);

            nv.utils.windowResize(MonitorGraphUI.ChartLatency.update);

            return MonitorGraphUI.ChartLatency;
        });

        nv.addGraph(function () {

            MonitorGraphUI.ChartTransactions.xAxis
                .tickFormat(function (d) {
                    return d3.time.format('%X')(new Date(d));
                });
            
            MonitorGraphUI.ChartTransactions.xAxis.rotateLabels(-20);
            
            MonitorGraphUI.ChartTransactions.yAxis
                .tickFormat(d3.format(',.2f'));

            MonitorGraphUI.ChartTransactions.yAxis
                .axisLabel('(Transactions/s)')
                .axisLabelDistance(10);

            MonitorGraphUI.ChartTransactions.margin({ left: 80 });

            MonitorGraphUI.ChartTransactions.tooltipContent(function (key, y, e, graph) {
                return '<h3> Transactions </h3>'
                    + '<p>' + e + ' tps at ' + y + '</p>';
            });

            d3.select('#visualisationTransaction')
                .datum(dataTransactions)
                .transition().duration(500)
                .call(MonitorGraphUI.ChartTransactions);
            
            nv.utils.windowResize(MonitorGraphUI.ChartTransactions.update);

            return MonitorGraphUI.ChartTransactions;
        });

        function Histogram(lowestTrackableValue, highestTrackableValue, nSVD, totalCount) {
            this.lowestTrackableValue = lowestTrackableValue;
            this.highestTrackableValue = highestTrackableValue;
            this.nSVD = nSVD;
            this.totalCount = totalCount;
            this.count = [];
            this.init();
        }

        Histogram.prototype.init = function() {
            var largestValueWithSingleUnitResolution = 2 * Math.pow(10, this.nSVD);
            this.unitMagnitude = Math.floor(Math.log(this.lowestTrackableValue) / Math.log(2));
            var subBucketCountMagnitude = Math.ceil(Math.log(largestValueWithSingleUnitResolution) / Math.log(2));
            this.subBucketHalfCountMagnitude = ((subBucketCountMagnitude > 1) ? subBucketCountMagnitude : 1) - 1;
            this.subBucketCount = Math.pow(2, (this.subBucketHalfCountMagnitude + 1));
            this.subBucketHalfCount = this.subBucketCount / 2;
            this.subBucketMask = (this.subBucketCount - 1) << this.unitMagnitude;
            var trackableValue = (this.subBucketCount - 1) << this.unitMagnitude;
            var bucketsNeeded = 1;
            while (trackableValue < this.highestTrackableValue) {
                trackableValue *= 2;
                bucketsNeeded++;
            }
            this.bucketCount = bucketsNeeded;

            this.countsArrayLength = (this.bucketCount + 1) * (this.subBucketCount / 2);
        };

        Histogram.prototype.diff = function(newer) {
            var h = new Histogram(newer.lowestTrackableValue, newer.highestTrackableValue, newer.nSVD, newer.totalCount - this.totalCount);
            for (var i = 0; i < h.countsArrayLength; i++) {
                h.count[i] = newer.count[i] - this.count[i];
            }
            return h;
        };

        Histogram.prototype.getCountAt = function(bucketIndex, subBucketIndex) {
            var bucketBaseIndex = (bucketIndex + 1) << this.subBucketHalfCountMagnitude;
            var offsetInBucket = subBucketIndex - this.subBucketHalfCount;
            var countIndex = bucketBaseIndex + offsetInBucket;
            return this.count[countIndex];
        };

        Histogram.prototype.valueFromIndex = function(bucketIndex, subBucketIndex) {
            return subBucketIndex * Math.pow(2, bucketIndex + this.unitMagnitude);
        };

        Histogram.prototype.getValueAtPercentile = function(percentile) {
            var totalToCurrentIJ = 0;
            var countAtPercentile = Math.floor(((percentile / 100.0) * this.totalCount) + 0.5); // round to nearest
            for (var i = 0; i < this.bucketCount; i++) {
                var j = (i == 0) ? 0 : (this.subBucketCount / 2);
                for (; j < this.subBucketCount; j++) {
                    totalToCurrentIJ += this.getCountAt(i, j);
                    if (totalToCurrentIJ >= countAtPercentile) {
                        var valueAtIndex = this.valueFromIndex(i, j);
                        return valueAtIndex / 1000.0;
                    }
                }
            }
        };

        function read32(str) {
            var s1 = str.substring(0, 2);
            var s2 = str.substring(2, 4);
            var s3 = str.substring(4, 6);
            var s4 = str.substring(6, 8);
            return s4 + s3 + s2 + s1;
        }

        function read64(str) {
            var s1 = read32(str);
            var s2 = read32(str.substring(8, 16));
            return s2 + s1;
        }

        function convert2Histogram(str) {
            // Read lowestTrackableValue
            var lowestTrackableValue = parseInt(read64(str), 16);
            str = str.substring(16, str.length);

            // Read highestTrackableValue
            var highestTrackableValue = parseInt(read64(str), 16);
            str = str.substring(16, str.length);

            // Read numberOfSignificantValueDigits
            var nSVD = parseInt(read32(str), 16);
            str = str.substring(8, str.length);

            // Read totalCount
            var totalCount = parseInt(read64(str), 16);
            str = str.substring(16, str.length);

            var histogram = new Histogram(lowestTrackableValue, highestTrackableValue, nSVD, totalCount);

            var i = 0;
            while (str.length >= 16) {
                var value = parseInt(read64(str), 16);
                histogram.count[i] = value;
                str = str.substring(16, str.length);
                i++;
            }
            return histogram;
        }

        var getEmptyDataForView = function (view) {
            view = view != undefined ? view.toLowerCase() : "seconds";

            if (view == "minutes")
                return getEmptyDataForMinutes();
            else if (view == "days")
                return getEmptyDataForDays();

            return getEmptyData();
        };

        this.AddGraph = function (view, cpuChartObj, ramChartObj, clusterChartObj, transactinoChartObj) {
            cpuChart = cpuChartObj;
            ramChart = ramChartObj;
            latencyChart = clusterChartObj;
            transactionChart = transactinoChartObj;
            currentView = view;
            MonitorGraphUI.Monitors = {                
                'latHistogram': null,
                'latData': getEmptyData(),
                'latDataMin': getEmptyDataForMinutes(),
                'latDataDay': getEmptyDataForDays(),
                'latFirstData': true,
                'tpsData': getEmptyData(),
                'tpsDataMin': getEmptyDataForMinutes(),
                'tpsDataDay': getEmptyDataForDays(),
                'tpsFirstData': true,
                'memData': getEmptyData(),
                'memDataMin': getEmptyDataForMinutes(),
                'memDataDay': getEmptyDataForDays(),
                'memFirstData': true,
                'cpuData': getEmptyData(),
                'cpuDataMin': getEmptyDataForMinutes(),
                'cpuDataHrs': getEmptyDataForDays(),
                'cpuFirstData': true,
                'lastTimedTransactionCount': -1,
                'lastTimerTick': -1
            };
            
            dataCpu[0]["values"] = getEmptyDataForView(view);
            dataRam[0]["values"] = getEmptyDataForView(view);
            dataLatency[0]["values"] = getEmptyDataForView(view);
            dataTransactions[0]["values"] = getEmptyDataForView(view);
            changeAxisTimeFormat(view);
        };

        this.RefreshGraph = function(view) {
            if (view == 'Days') {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuDataHrs;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsDataDay;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memDataDay;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latDataDay;
            } else if (view == 'Minutes') {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuDataMin;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsDataMin;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memDataMin;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latDataMin;
            } else {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuData;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsData;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memData;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latData;
            }
            d3.select('#visualisationCpu')
                .datum(dataCpu)
                .transition().duration(500)
                .call(MonitorGraphUI.ChartCpu);

            nv.utils.windowResize(MonitorGraphUI.ChartCpu.update);
            changeAxisTimeFormat(view);
        };

        this.UpdateCharts = function() {

            if (ramChart.is(":visible"))
                MonitorGraphUI.ChartRam.update();

            if (cpuChart.is(":visible"))
                MonitorGraphUI.ChartCpu.update();

            if (latencyChart.is(":visible"))
                MonitorGraphUI.ChartLatency.update();

            if (transactionChart.is(":visible"))
                MonitorGraphUI.ChartTransactions.update();
        };

        var changeAxisTimeFormat = function (view) {
            var dateFormat = '%X';
            if (view == 'Days')
                dateFormat = '%d %b %X';

            MonitorGraphUI.ChartCpu.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
            MonitorGraphUI.ChartRam.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
            MonitorGraphUI.ChartLatency.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
            MonitorGraphUI.ChartTransactions.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
        };

        this.RefreshLatency = function (latency, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var dataLat = monitor.latData;
            var dataLatMin = monitor.latDataMin;
            var dataLatDay = monitor.latDataDay;
            var strLatStats = "";
            var timeStamp;

            // Compute latency statistics
            jQuery.each(latency, function (id, val) {
                strLatStats += val["UNCOMPRESSED_HISTOGRAM"];
                timeStamp = val["TIMESTAMP"];
            });

            var latStats = convert2Histogram(strLatStats);
            
            var lat = 0;
            if (monitor.latHistogram == null)
                lat = latStats.getValueAtPercentile(99);
            else
                lat = monitor.latHistogram.diff(latStats).getValueAtPercentile(99);
            lat = parseFloat(lat).toFixed(1) * 1;
            monitor.latHistogram = latStats;

            if (latSecCount == 6 || monitor.latFirstData) {
                dataLatMin = dataLatMin.slice(1);
                dataLatMin.push({ 'x': new Date(timeStamp), 'y': lat });
                MonitorGraphUI.Monitors.latDataMin = dataLatMin;
                latSecCount = 0;
            }

            if (latMinCount == 120 || monitor.latFirstData) {
                dataLatDay = dataLatDay.slice(1);
                dataLatDay.push({ 'x': new Date(timeStamp), 'y': lat });
                MonitorGraphUI.Monitors.latDataDay = dataLatDay;
                latMinCount = 0;
            }

            dataLat = dataLat.slice(1);
            dataLat.push({ 'x': new Date(timeStamp), 'y': lat });
            MonitorGraphUI.Monitors.latData = dataLat;

            if (graphView == 'Minutes')
                dataLatency[0]["values"] = dataLatMin;
            else if (graphView == 'Days')
                dataLatency[0]["values"] = dataLatDay;
            else
                dataLatency[0]["values"] = dataLat;

            if (currentTab == NavigationTabs.DBMonitor && latencyChart.is(":visible")) {
                d3.select("#visualisationLatency")
                    .datum(dataLatency)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartLatency);
            }
            monitor.latFirstData = false;
            latSecCount++;
            latMinCount++;
        };

        this.RefreshMemory = function (memoryDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var dataMem = monitor.memData;
            var dataMemMin = monitor.memDataMin;
            var dataMemDay = monitor.memDataDay;
            var memDetails = memoryDetails;
            var memRss = parseFloat(memDetails[currentServer].RSS * 1.0 / 1048576.0).toFixed(3) * 1;
            var memTimeStamp = new Date(memDetails[currentServer].TIMESTAMP);

            if (memSecCount == 6 || monitor.memFirstData) {
                dataMemMin = dataMemMin.slice(1);
                dataMemMin.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                MonitorGraphUI.Monitors.memDataMin = dataMemMin;
                memSecCount = 0;
            }

            if (memMinCount == 120 || monitor.memFirstData) {
                dataMemDay = dataMemDay.slice(1);
                dataMemDay.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                MonitorGraphUI.Monitors.memDataDay = dataMemDay;
                memMinCount = 0;
            }

            dataMem = dataMem.slice(1);
            dataMem.push({ 'x': new Date(memTimeStamp), 'y': memRss });
            MonitorGraphUI.Monitors.memData = dataMem;

            if (graphView == 'Minutes')
                dataRam[0]["values"] = dataMemMin;
            else if (graphView == 'Days')
                dataRam[0]["values"] = dataMemDay;
            else
                dataRam[0]["values"] = dataMem;

            if (currentTab == NavigationTabs.DBMonitor && ramChart.is(":visible")) {
                d3.select('#visualisationRam')
                    .datum(dataRam)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartRam);
            }
            monitor.memFirstData = false;
            memSecCount++;
            memMinCount++;
        };

        this.RefreshTransaction = function (transactionDetails, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var datatrans = monitor.tpsData;
            var datatransMin = monitor.tpsDataMin;
            var datatransDay = monitor.tpsDataDay;
            var transacDetail = transactionDetails;
            var currentTimedTransactionCount = transacDetail["CurrentTimedTransactionCount"];
            var currentTimerTick = transacDetail["currentTimerTick"];

            if (monitor.lastTimedTransactionCount > 0 && monitor.lastTimerTick > 0 && monitor.lastTimerTick != currentTimerTick) {
                var delta = currentTimedTransactionCount - monitor.lastTimedTransactionCount;
                if (tpsSecCount == 6 || monitor.tpsFirstData) {
                    datatransMin = datatransMin.slice(1);
                    datatransMin.push({ "x": new Date(transacDetail["TimeStamp"]), "y": parseFloat(delta * 1000.0 / (currentTimerTick - monitor.lastTimerTick)).toFixed(1) * 1 });
                    MonitorGraphUI.Monitors.tpsDataMin = datatransMin;
                    tpsSecCount = 0;
                }
                if (tpsMinCount == 120 || monitor.tpsFirstData) {
                    datatransDay = datatransDay.slice(1);
                    datatransDay.push({ "x": new Date(transacDetail["TimeStamp"]), "y": parseFloat(delta * 1000.0 / (currentTimerTick - monitor.lastTimerTick)).toFixed(1) * 1 });
                    MonitorGraphUI.Monitors.tpsDataDay = datatransDay;
                    tpsMinCount = 0;
                }
                datatrans = datatrans.slice(1);
                datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": parseFloat(delta * 1000.0 / (currentTimerTick - monitor.lastTimerTick)).toFixed(1) * 1 });
                MonitorGraphUI.Monitors.tpsData = datatrans;
            }

            if (graphView == 'Minutes')
                dataTransactions[0]["values"] = datatransMin;
            else if (graphView == 'Days')
                dataTransactions[0]["values"] = datatransDay;
            else
                dataTransactions[0]["values"] = datatrans;

            monitor.lastTimedTransactionCount = currentTimedTransactionCount;
            monitor.lastTimerTick = currentTimerTick;
            monitor.tpsFirstData = false;

            if (currentTab == NavigationTabs.DBMonitor && transactionChart.is(":visible")) {
                d3.select('#visualisationTransaction')
                    .datum(dataTransactions)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartTransactions);
            }
            
            tpsSecCount++;
            tpsMinCount++;
        };

        this.RefreshCpu = function (cpuDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var cpuData = monitor.cpuData;
            var cpuDataMin = monitor.cpuDataMin;
            var cpuDataDay = monitor.cpuDataHrs;
            var cpuDetail = cpuDetails;
            var percentageUsage = parseFloat(cpuDetail[currentServer].PERCENT_USED).toFixed(1) * 1;
            var timeStamp = cpuDetail[currentServer].TIMESTAMP;

            if (cpuSecCount == 6 || monitor.cpuFirstData) {
                cpuDataMin = cpuDataMin.slice(1);
                cpuDataMin.push({ "x": new Date(timeStamp), "y": percentageUsage });
                MonitorGraphUI.Monitors.cpuDataMin = cpuDataMin;
                cpuSecCount = 0;
            }
            if (cpuMinCount == 120 || monitor.cpuFirstData) {
                cpuDataDay = cpuDataDay.slice(1);
                cpuDataDay.push({ "x": new Date(timeStamp), "y": percentageUsage });
                MonitorGraphUI.Monitors.cpuDataHrs = cpuDataDay;
                cpuMinCount = 0;
            }

            cpuData = cpuData.slice(1);
            cpuData.push({ "x": new Date(timeStamp), "y": percentageUsage });
            MonitorGraphUI.Monitors.cpuData = cpuData;
            monitor.cpuFirstData = false;

            if (graphView == 'Minutes')
                dataCpu[0]["values"] = cpuDataMin;
            else if (graphView == 'Days')
                dataCpu[0]["values"] = cpuDataDay;
            else {
                dataCpu[0]["values"] = cpuData;

            }

            if (currentTab == NavigationTabs.DBMonitor && cpuChart.is(":visible")) {
                d3.select('#visualisationCpu')
                    .datum(dataCpu)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCpu);
            }
            cpuSecCount++;
            cpuMinCount++;
        };
    });
    
    window.MonitorGraphUI = MonitorGraphUI = new IMonitorGraphUI();
})(window);

