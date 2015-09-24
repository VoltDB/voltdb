
(function (window) {

    var IMonitorGraphUI = (function () {

        var currentView = "Seconds";
        var cpuSecCount = 0;
        var cpuMinCount = 0;
        var cmdLogSecCount = 0;
        var cmdLogMinCount = 0;

        var tpsSecCount = 0;
        var tpsMinCount = 0;
        var memSecCount = 0;
        var memMinCount = 0;
        var latSecCount = 0;
        var latMinCount = 0;
        var partitionSecCount = 0;
        var partitionMinCount = 0;
        var drSecCount = 0;
        var drMinCount = 0;
        var totalEmptyData = 121;
        var totalEmptyDataForMinutes = 121;
        var totalEmptyDataForDays = 360;
        var cpuChart;
        var ramChart;
        var latencyChart;
        var transactionChart;
        var partitionChart;
        var drReplicationChart;        var cmdLogChart;        var cmdLogOverlay = [];        var physicalMemory = -1;
        this.Monitors = {};
        this.ChartCpu = nv.models.lineChart();
        this.ChartRam = nv.models.lineChart();
        this.ChartLatency = nv.models.lineChart();
        this.ChartTransactions = nv.models.lineChart();
        this.ChartPartitionIdleTime = nv.models.lineChart();
        this.ChartDrReplicationRate = nv.models.lineChart();        this.ChartCommandlog = nv.models.lineChart();        var dataMapperSec = {};
        var dataMapperMin = {};
        var dataMapperDay = {};

        this.GetPartitionDetailData = function (partitionDetails) {
            dataParitionDetails = partitionDetails;
        };

        function getEmptyData() {
            var arr = [];
            var theDate = new Date();

            for (var i = totalEmptyData; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y: null };
                theDate.setSeconds(theDate.getSeconds() - 5);
            }

            return arr;
        }

        function getEmptyDataForMinutes() {
            var arr = [];
            var theDate = new Date();

            for (var i = totalEmptyDataForMinutes; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y: null };
                theDate.setSeconds(theDate.getSeconds() - 30);
            }

            return arr;
        }

        function getEmptyDataForDays() {
            var arr = [];
            var theDate = new Date();

            for (var i = totalEmptyDataForDays; i >= 0; i--) {
                arr[i] = { x: new Date(theDate.getTime()), y: null };
                theDate.setMinutes(theDate.getMinutes() - 5);
            }

            return arr;
        }

        function getEmptyDataForPartition() {
            var count = 0;
            var dataPartition = [];

            if (dataParitionDetails != undefined) {
                $.each(dataParitionDetails, function (key, value) {
                    $.each(value, function (datatype, datatypeValue) {
                        $.each(datatypeValue, function (partitionKey, partitionValue) {
                            var arr = [];
                            arr.push(emptyData[0]);
                            arr.push(emptyData[emptyData.length - 1]);
                            if (datatype == "data") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#D3D3D3" });
                            } else if (datatype == "dataMPI") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#FF8C00" });
                            } else if (datatype == "dataMax" || datatype == "dataMin") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#4C76B0" });
                            }
                            dataMapperSec[partitionKey] = count;
                            count++;
                        });
                    });
                });
            }
            return dataPartition;
        };

        function getEmptyDataForPartitionForMinutes() {
            var count = 0;
            var dataPartition = [];
            if (dataParitionDetails != undefined) {
                $.each(dataParitionDetails, function (key, value) {
                    $.each(value, function (datatype, datatypeValue) {
                        $.each(datatypeValue, function (partitionKey, partitionValue) {
                            var arr = [];
                            arr.push(emptyDataForMinutes[0]);
                            arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);
                            if (datatype == "data") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#D3D3D3" });
                            } else if (datatype == "dataMPI") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#FF8C00" });
                            } else if (datatype == "dataMax" || datatype == "dataMin") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#4C76B0" });
                            }
                            dataMapperMin[partitionKey] = count;
                            count++;
                        });
                    });
                });
            }
            return dataPartition;
        };

        function getEmptyDataForPartitionForDay() {
            var count = 0;
            var dataPartition = [];
            if (dataParitionDetails != undefined) {
                $.each(dataParitionDetails, function (key, value) {
                    $.each(value, function (datatype, datatypeValue) {
                        $.each(datatypeValue, function (partitionKey, partitionValue) {
                            var arr = [];
                            arr.push(emptyDataForDays[0]);
                            arr.push(emptyDataForDays[emptyDataForDays.length - 1]);
                            if (datatype == "data") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#D3D3D3" });
                            } else if (datatype == "dataMPI") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#FF8C00" });
                            } else if (datatype == "dataMax" || datatype == "dataMin") {
                                dataPartition.push({ key: partitionKey, values: arr, color: "#4C76B0" });
                            }
                            dataMapperDay[partitionKey] = count;
                            count++;
                        });
                    });
                });
            }
            return dataPartition;
        };

        var emptyData = getEmptyData();
        var emptyDataForMinutes = getEmptyDataForMinutes();
        var emptyDataForDays = getEmptyDataForDays();

        function getEmptyDataOptimized() {
            var arr = [];
            arr.push(emptyData[0]);
            arr.push(emptyData[emptyData.length - 1]);

            return arr;
        }

        function getEmptyDataForMinutesOptimized() {
            var arr = [];
            arr.push(emptyDataForMinutes[0]);
            arr.push(emptyDataForMinutes[emptyDataForMinutes.length - 1]);

            return arr;
        }

        function getEmptyDataForDaysOptimized() {
            var arr = [];
            arr.push(emptyDataForDays[0]);
            arr.push(emptyDataForDays[emptyDataForDays.length - 1]);

            return arr;
        }

        var dataCpu = [{
            "key": "CPU",
            "values": getEmptyDataOptimized(),
            "color": "rgb(164, 136, 5)"
        }];

        var dataRam = [{
            "key": "RAM",
            "values": getEmptyDataOptimized(),
            "color": "rgb(164, 136, 5)"
        }];

        var dataLatency = [{
            "key": "Latency",
            "values": getEmptyDataOptimized(),
            "color": "rgb(27, 135, 200)"
        }];

        var dataTransactions = [{
            "key": "Transactions",
            "values": getEmptyDataOptimized(),
            "color": "rgb(27, 135, 200)"
        }];

        var dataDrReplicationRate = [{
            "key": "Replication Rate",
            "values": getEmptyDataOptimized(),
            "color": "rgb(27, 135, 200)"
        }];

        var dataCommandLog = [{
            "key": "Command Log Statistics",
            "values": getEmptyDataOptimized(),
            "color": "rgb(27, 135, 200)"
        }];

        var dataPartitionIdleTime = [];

        var dataParitionDetails = [];

        nv.addGraph({
            generate: function() {
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
                
                MonitorGraphUI.ChartCpu.margin({ left: 80 });
                MonitorGraphUI.ChartCpu.yAxis.scale().domain([0, 100]);
                MonitorGraphUI.ChartCpu.lines.forceY([0, 100]);

                d3.select('#visualisationCpu')
                    .datum(dataCpu)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCpu);

                nv.utils.windowResize(MonitorGraphUI.ChartCpu.update);
                
                return MonitorGraphUI.ChartCpu;
            },
            callback: function (p) {
                MonitorGraphUI.ChartRam.useInteractiveGuideline(false);
                var tooltip = MonitorGraphUI.ChartCpu.tooltip;
                tooltip.gravity('s');
                tooltip.contentGenerator(function (d) {
                    console.log(d);
                    var html = '';
                    d.series.forEach(function (elem) {
                        html += "<h3>"
                            + elem.key + "</h3>";
                    });
                    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + "% at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                    return html;
                });
                return MonitorGraphUI.ChartCpu;
            }
        });

        nv.addGraph({
            generate: function() {
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
                MonitorGraphUI.ChartRam.lines.forceY([0, 0.1]);

                d3.select('#visualisationRam')
                    .datum(dataRam)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartRam);

                nv.utils.windowResize(MonitorGraphUI.ChartRam.update);
            },
            callback: function (p) {
                MonitorGraphUI.ChartRam.useInteractiveGuideline(false);
                var tooltip = MonitorGraphUI.ChartRam.tooltip;

                tooltip.contentGenerator(function (d) {
                    var html = '';
                    d.series.forEach(function (elem) {
                        html += "<h3>"
                            + elem.key + "</h3>";
                    });
                    html = html + "<h2>" + parseFloat(d.point.y).toFixed(4) + " GB at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                    return html;
                });
                return MonitorGraphUI.ChartCpu;
            }
        });

        nv.addGraph({
            generate: function() {
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
                MonitorGraphUI.ChartLatency.lines.forceY([0, 1]);

                d3.select('#visualisationLatency')
                    .datum(dataLatency)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartLatency);

                nv.utils.windowResize(MonitorGraphUI.ChartLatency.update);
            },
            callback: function(p) {
                MonitorGraphUI.ChartLatency.useInteractiveGuideline(false);
                var tooltip = MonitorGraphUI.ChartLatency.tooltip;
               
                tooltip.contentGenerator(function (d) {
                    var html = '';
                    d.series.forEach(function (elem) {
                        html += "<h3>"
                            + elem.key + "</h3>";
                    });
                    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " ms at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                    return html;
                });
                return MonitorGraphUI.ChartLatency;
            }
        });


        nv.addGraph({            
           generate: function() {
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
               MonitorGraphUI.ChartTransactions.lines.forceY([0, 1]);

               d3.select('#visualisationTransaction')
                   .datum(dataTransactions)
                   .transition().duration(500)
                   .call(MonitorGraphUI.ChartTransactions);

               nv.utils.windowResize(MonitorGraphUI.ChartTransactions.update);
           },
           callback: function(p) {
               MonitorGraphUI.ChartTransactions.useInteractiveGuideline(false);
               var tooltip = MonitorGraphUI.ChartTransactions.tooltip;
               
               tooltip.contentGenerator(function (d) {
                   var html = '';
                   d.series.forEach(function (elem) {
                       html += "<h3>"
                           + elem.key + "</h3>";
                   });
                   html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " tps at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                   return html;
               });
               return MonitorGraphUI.ChartTransactions;
           }
        });


        nv.addGraph({            
            generate:function() {
                MonitorGraphUI.ChartPartitionIdleTime.xAxis
                .tickFormat(function (d) {
                    return d3.time.format('%X')(new Date(d));
                });

                MonitorGraphUI.ChartPartitionIdleTime.showLegend(false);
                MonitorGraphUI.ChartPartitionIdleTime.xAxis.rotateLabels(-20);

                MonitorGraphUI.ChartPartitionIdleTime.yAxis
                    .tickFormat(d3.format(',.2f'));

                MonitorGraphUI.ChartPartitionIdleTime.yAxis
                    .axisLabel('(%)')
                    .axisLabelDistance(10);

                MonitorGraphUI.ChartPartitionIdleTime.margin({ left: 80 });
                MonitorGraphUI.ChartPartitionIdleTime.yAxis.scale().domain([0, 100]);
                MonitorGraphUI.ChartPartitionIdleTime.lines.forceY([0, 100]);

                d3.select('#visualisationPartitionIdleTime')
                    .datum([])
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartPartitionIdleTime);

                nv.utils.windowResize(MonitorGraphUI.ChartPartitionIdleTime.update);
            },
            callback: function () {
                MonitorGraphUI.ChartPartitionIdleTime.useInteractiveGuideline(true);
                return MonitorGraphUI.ChartPartitionIdleTime;
            }
        });


        nv.addGraph({
            generate:function() {
                MonitorGraphUI.ChartDrReplicationRate.xAxis
               .tickFormat(function (d) {
                   return d3.time.format('%X')(new Date(d));
               });

                MonitorGraphUI.ChartDrReplicationRate.xAxis.rotateLabels(-20);

                MonitorGraphUI.ChartDrReplicationRate.yAxis
                    .tickFormat(d3.format(',.2f'));

                MonitorGraphUI.ChartDrReplicationRate.yAxis
                    .axisLabel('(KBps)')
                    .axisLabelDistance(10);

                MonitorGraphUI.ChartDrReplicationRate.margin({ left: 80 });
                MonitorGraphUI.ChartDrReplicationRate.lines.forceY([0, 1]);

                d3.select('#visualizationDrReplicationRate')
                    .datum(dataDrReplicationRate)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartDrReplicationRate);

                nv.utils.windowResize(MonitorGraphUI.ChartDrReplicationRate.update);
            },
            callback: function() {
                MonitorGraphUI.ChartDrReplicationRate.useInteractiveGuideline(false);
                var tooltip = MonitorGraphUI.ChartDrReplicationRate.tooltip;
                tooltip.contentGenerator(function (d) {
                    var html = '';
                    d.series.forEach(function (elem) {
                        html += "<h3>"
                            + elem.key + "</h3>";
                    });
                    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " KBps at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                    return html;
                });
                return MonitorGraphUI.ChartDrReplicationRate;
            }
        });


        nv.addGraph({            
            generate: function () {
                MonitorGraphUI.ChartCommandlog.showLegend(false);
                MonitorGraphUI.ChartCommandlog.xAxis
                    .tickFormat(function (d) {
                        return d3.time.format('%X')(new Date(d));
                    });

                MonitorGraphUI.ChartCommandlog.xAxis.rotateLabels(-20);

                MonitorGraphUI.ChartCommandlog.yAxis
                    .tickFormat(d3.format(',.2f'));

                MonitorGraphUI.ChartCommandlog.yAxis
                    .axisLabel('(Pending Transactions)')
                    .axisLabelDistance(10);

                MonitorGraphUI.ChartCommandlog.margin({ left: 80 });
                MonitorGraphUI.ChartCommandlog.lines.forceY([0, 0.1]);

                d3.select('#visualisationCommandLog')
                    .datum(dataCommandLog)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCommandlog);

                nv.utils.windowResize(MonitorGraphUI.ChartCommandlog.update);
           },
            callback:function() {
               MonitorGraphUI.ChartCommandlog.useInteractiveGuideline(false);
               var tooltip = MonitorGraphUI.ChartCommandlog.tooltip;
               tooltip.contentGenerator(function (d) {
                   var html = '';
                   d.series.forEach(function (elem) {
                       html += "<h3>"
                           + elem.key + "</h3>";
                   });
                   html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " Pending at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                   return html;
               });
               return MonitorGraphUI.ChartCommandlog;
           }
        });

        function Histogram(lowestTrackableValue, highestTrackableValue, nSVD, totalCount) {
            this.lowestTrackableValue = lowestTrackableValue;
            this.highestTrackableValue = highestTrackableValue;
            this.nSVD = nSVD;
            this.totalCount = totalCount;
            this.count = [];
            this.init();
        }

        Histogram.prototype.init = function () {
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

        Histogram.prototype.diff = function (newer) {
            var h = new Histogram(newer.lowestTrackableValue, newer.highestTrackableValue, newer.nSVD, newer.totalCount - this.totalCount);
            for (var i = 0; i < h.countsArrayLength; i++) {
                h.count[i] = newer.count[i] - this.count[i];
            }
            return h;
        };

        Histogram.prototype.getCountAt = function (bucketIndex, subBucketIndex) {
            var bucketBaseIndex = (bucketIndex + 1) << this.subBucketHalfCountMagnitude;
            var offsetInBucket = subBucketIndex - this.subBucketHalfCount;
            var countIndex = bucketBaseIndex + offsetInBucket;
            return this.count[countIndex];
        };

        Histogram.prototype.valueFromIndex = function (bucketIndex, subBucketIndex) {
            return subBucketIndex * Math.pow(2, bucketIndex + this.unitMagnitude);
        };

        Histogram.prototype.getValueAtPercentile = function (percentile) {
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
                return getEmptyDataForMinutesOptimized();
            else if (view == "days")
                return getEmptyDataForDaysOptimized();

            return getEmptyDataOptimized();
        };

        var getEmptyDataForPartitionView = function (view) {
            view = view != undefined ? view.toLowerCase() : "seconds";

            if (view == "minutes")
                return getEmptyDataForPartitionForMinutes();
            else if (view == "days")
                return getEmptyDataForPartitionForDay();

            return getEmptyDataForPartition();
        };

        this.AddGraph = function (view, cpuChartObj, ramChartObj, clusterChartObj, transactinoChartObj, partitionChartObj, drReplicationCharObj, cmdLogChartObj) {
            cpuChart = cpuChartObj;
            ramChart = ramChartObj;
            latencyChart = clusterChartObj;
            transactionChart = transactinoChartObj;
            partitionChart = partitionChartObj;
            drReplicationChart = drReplicationCharObj;
            cmdLogChart = cmdLogChartObj;            currentView = view;
            MonitorGraphUI.Monitors = {
                'latHistogram': {},
                'latData': getEmptyDataOptimized(),
                'latDataMin': getEmptyDataForMinutesOptimized(),
                'latDataDay': getEmptyDataForDaysOptimized(),
                'latFirstData': true,
                'tpsData': getEmptyDataOptimized(),
                'tpsDataMin': getEmptyDataForMinutesOptimized(),
                'tpsDataDay': getEmptyDataForDaysOptimized(),
                'tpsFirstData': true,
                'memData': getEmptyDataOptimized(),
                'memDataMin': getEmptyDataForMinutesOptimized(),
                'memDataDay': getEmptyDataForDaysOptimized(),
                'memFirstData': true,
                'cpuData': getEmptyDataOptimized(),
                'cpuDataMin': getEmptyDataForMinutesOptimized(),
                'cpuDataHrs': getEmptyDataForDaysOptimized(),
                'cpuFirstData': true,
                'partitionData': getEmptyDataForPartition(),
                'partitionDataMin': getEmptyDataForPartitionForMinutes(),
                'partitionDataDay': getEmptyDataForPartitionForDay(),
                'partitionFirstData': true,
                'drReplicationData': getEmptyDataOptimized(),
                'drReplicationDataMin': getEmptyDataForMinutesOptimized(),
                'drReplicationDataDay': getEmptyDataForDaysOptimized(),                //pm
                'cmdLogData': getEmptyDataOptimized(),
                'cmdLogDataMin': getEmptyDataForMinutesOptimized(),                'cmdLogDataDay': getEmptyDataForDaysOptimized(),                'cmdLogFirstData': true,                'drFirstData': true,                'lastTimedTransactionCount': -1,
                'lastTimerTick': -1
            };

            dataCpu[0]["values"] = getEmptyDataForView(view);
            dataRam[0]["values"] = getEmptyDataForView(view);
            dataLatency[0]["values"] = getEmptyDataForView(view);
            dataTransactions[0]["values"] = getEmptyDataForView(view);
            dataPartitionIdleTime = getEmptyDataForPartitionView(view);
            dataDrReplicationRate[0]["values"] = getEmptyDataForView(view);
            dataCommandLog[0]["values"] = getEmptyDataForView(view);
            changeAxisTimeFormat(view);
        };

        this.RefreshGraph = function (view) {
            currentView = view;
            if (view == 'Days') {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuDataHrs;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsDataDay;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memDataDay;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latDataDay;
                dataPartitionIdleTime = MonitorGraphUI.Monitors.partitionDataDay;
                dataDrReplicationRate[0]["values"] = MonitorGraphUI.Monitors.drReplicationDataDay;
                dataCommandLog[0]["values"] = MonitorGraphUI.Monitors.cmdLogDataDay;
            } else if (view == 'Minutes') {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuDataMin;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsDataMin;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memDataMin;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latDataMin;
                dataPartitionIdleTime = MonitorGraphUI.Monitors.partitionDataMin;
                dataDrReplicationRate[0]["values"] = MonitorGraphUI.Monitors.drReplicationDataMin;
                dataCommandLog[0]["values"] = MonitorGraphUI.Monitors.cmdLogDataMin;
            } else {
                dataCpu[0]["values"] = MonitorGraphUI.Monitors.cpuData;
                dataTransactions[0]["values"] = MonitorGraphUI.Monitors.tpsData;
                dataRam[0]["values"] = MonitorGraphUI.Monitors.memData;
                dataLatency[0]["values"] = MonitorGraphUI.Monitors.latData;
                dataPartitionIdleTime = MonitorGraphUI.Monitors.partitionData;
                dataDrReplicationRate[0]["values"] = MonitorGraphUI.Monitors.drReplicationData;
                dataCommandLog[0]["values"] = MonitorGraphUI.Monitors.cmdLogData;
            }

            nv.utils.windowResize(MonitorGraphUI.ChartCpu.update);
            changeAxisTimeFormat(view);
        };

        this.UpdateCharts = function () {

            if (ramChart.is(":visible"))
                MonitorGraphUI.ChartRam.update();

            if (cpuChart.is(":visible"))
                MonitorGraphUI.ChartCpu.update();

            if (latencyChart.is(":visible"))
                MonitorGraphUI.ChartLatency.update();

            if (transactionChart.is(":visible"))
                MonitorGraphUI.ChartTransactions.update();

            if (partitionChart.is(":visible"))
                MonitorGraphUI.ChartPartitionIdleTime.update();

            if (drReplicationChart.is(":visible"))                MonitorGraphUI.ChartDrReplicationRate.update();

            if (cmdLogChart.is(":visible"))
                MonitorGraphUI.ChartCommandlog.update();
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
            MonitorGraphUI.ChartPartitionIdleTime.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
            MonitorGraphUI.ChartDrReplicationRate.xAxis
                .tickFormat(function (d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
            MonitorGraphUI.ChartCommandlog.xAxis
                .tickFormat(function(d) {
                    return d3.time.format(dateFormat)(new Date(d));
                });
        };

        var dataView = {
            'Seconds': 0,
            'Minutes': 1,
            'Days': 2
        };

        function sliceFirstData(dataArray, view) {

            var total = totalEmptyData;
            var refEmptyData = emptyData;

            if (view == dataView.Minutes) {
                total = totalEmptyDataForMinutes;
                refEmptyData = emptyDataForMinutes;
            }
            else if (view == dataView.Days) {
                total = totalEmptyDataForDays;
                refEmptyData = emptyDataForDays;
            }

            if (dataArray.length <= total)
                dataArray[0] = refEmptyData[dataArray.length - 1];
            else
                dataArray = dataArray.slice(1);

            return dataArray;
        }

        this.RefreshLatency = function (latency, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var dataLat = monitor.latData;
            var dataLatMin = monitor.latDataMin;
            var dataLatDay = monitor.latDataDay;
            var timeStamp;
            var maxLatency = 0;

            // Compute latency statistics
            jQuery.each(latency, function (id, val) {
                var strLatStats = val["UNCOMPRESSED_HISTOGRAM"];
                timeStamp = val["TIMESTAMP"];
                var latStats = convert2Histogram(strLatStats);

                var singlelat = 0;
                if (!monitor.latHistogram.hasOwnProperty(id))
                    singlelat = latStats.getValueAtPercentile(99);
                else
                    singlelat = monitor.latHistogram[id].diff(latStats).getValueAtPercentile(99);
                singlelat = parseFloat(singlelat).toFixed(1) * 1;

                if (singlelat > maxLatency) {
                    maxLatency = singlelat;
                }

                monitor.latHistogram[id] = latStats;
            });

            var lat = maxLatency;
            if (lat < 0)
                lat = 0;

            if (latSecCount >= 6 || monitor.latFirstData) {
                dataLatMin = sliceFirstData(dataLatMin, dataView.Minutes);
                dataLatMin.push({ 'x': new Date(timeStamp), 'y': lat });
                MonitorGraphUI.Monitors.latDataMin = dataLatMin;
                latSecCount = 0;
            }

            if (latMinCount >= 60 || monitor.latFirstData) {
                dataLatDay = sliceFirstData(dataLatDay, dataView.Days);
                dataLatDay.push({ 'x': new Date(timeStamp), 'y': lat });
                MonitorGraphUI.Monitors.latDataDay = dataLatDay;
                latMinCount = 0;
            }

            dataLat = sliceFirstData(dataLat, dataView.Seconds);
            dataLat.push({ 'x': new Date(timeStamp), 'y': lat });
            MonitorGraphUI.Monitors.latData = dataLat;

            if (graphView == 'Minutes')
                dataLatency[0]["values"] = dataLatMin;
            else if (graphView == 'Days')
                dataLatency[0]["values"] = dataLatDay;
            else
                dataLatency[0]["values"] = dataLat;

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && latencyChart.is(":visible")) {
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

            if ($.isEmptyObject(memDetails) || memDetails == undefined || memDetails[currentServer].PHYSICALMEMORY == undefined || memDetails[currentServer].RSS == undefined || memDetails[currentServer].TIMESTAMP == undefined)
                return;

            var memRss = parseFloat(memDetails[currentServer].RSS * 1.0 / 1048576.0).toFixed(3) * 1;
            var memTimeStamp = new Date(memDetails[currentServer].TIMESTAMP);

            if (memDetails[currentServer].PHYSICALMEMORY != -1 && physicalMemory != memDetails[currentServer].PHYSICALMEMORY) {
                physicalMemory = parseFloat(memDetails[currentServer].PHYSICALMEMORY * 1.0 / 1048576.0).toFixed(3) * 1;

                MonitorGraphUI.ChartRam.yAxis.scale().domain([0, physicalMemory]);
                MonitorGraphUI.ChartRam.lines.forceY([0, physicalMemory]);
            }

            if (memRss < 0)
                memRss = 0;
            else if (physicalMemory != -1 && memRss > physicalMemory)
                memRss = physicalMemory;

            if (memSecCount >= 6 || monitor.memFirstData) {
                dataMemMin = sliceFirstData(dataMemMin, dataView.Minutes);
                dataMemMin.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                MonitorGraphUI.Monitors.memDataMin = dataMemMin;
                memSecCount = 0;
            }

            if (memMinCount >= 60 || monitor.memFirstData) {
                dataMemDay = sliceFirstData(dataMemDay, dataView.Days);
                dataMemDay.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                MonitorGraphUI.Monitors.memDataDay = dataMemDay;
                memMinCount = 0;
            }

            dataMem = sliceFirstData(dataMem, dataView.Seconds);
            dataMem.push({ 'x': new Date(memTimeStamp), 'y': memRss });
            MonitorGraphUI.Monitors.memData = dataMem;

            if (graphView == 'Minutes')
                dataRam[0]["values"] = dataMemMin;
            else if (graphView == 'Days')
                dataRam[0]["values"] = dataMemDay;
            else
                dataRam[0]["values"] = dataMem;

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && ramChart.is(":visible")) {
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

            if ($.isEmptyObject(transacDetail) || transacDetail == undefined || transacDetail["CurrentTimedTransactionCount"] == undefined || transacDetail["TimeStamp"] == undefined || transacDetail["currentTimerTick"] == undefined)
                return;

            var currentTimedTransactionCount = transacDetail["CurrentTimedTransactionCount"];
            var currentTimerTick = transacDetail["currentTimerTick"];

            if (monitor.lastTimedTransactionCount > 0 && monitor.lastTimerTick > 0 && monitor.lastTimerTick != currentTimerTick) {
                var delta = currentTimedTransactionCount - monitor.lastTimedTransactionCount;
                var calculatedValue = parseFloat(delta * 1000.0 / (currentTimerTick - monitor.lastTimerTick)).toFixed(1) * 1;

                if (calculatedValue < 0 || isNaN(calculatedValue) || (currentTimerTick - monitor.lastTimerTick == 0))
                    calculatedValue = 0;

                if (tpsSecCount >= 6 || monitor.tpsFirstData) {
                    datatransMin = sliceFirstData(datatransMin, dataView.Minutes);
                    datatransMin.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                    MonitorGraphUI.Monitors.tpsDataMin = datatransMin;
                    tpsSecCount = 0;
                }
                if (tpsMinCount >= 60 || monitor.tpsFirstData) {
                    datatransDay = sliceFirstData(datatransDay, dataView.Days);
                    datatransDay.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                    MonitorGraphUI.Monitors.tpsDataDay = datatransDay;
                    tpsMinCount = 0;
                }
                datatrans = sliceFirstData(datatrans, dataView.Seconds);
                datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                MonitorGraphUI.Monitors.tpsData = datatrans;
                monitor.tpsFirstData = false;
            }

            if (graphView == 'Minutes')
                dataTransactions[0]["values"] = datatransMin;
            else if (graphView == 'Days')
                dataTransactions[0]["values"] = datatransDay;
            else
                dataTransactions[0]["values"] = datatrans;

            monitor.lastTimedTransactionCount = currentTimedTransactionCount;
            monitor.lastTimerTick = currentTimerTick;

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && transactionChart.is(":visible")) {
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

            if ($.isEmptyObject(cpuDetail) || cpuDetail == undefined || cpuDetail[currentServer].PERCENT_USED == undefined || cpuDetail[currentServer].TIMESTAMP == undefined)
                return;

            var percentageUsage = parseFloat(cpuDetail[currentServer].PERCENT_USED).toFixed(1) * 1;
            var timeStamp = cpuDetail[currentServer].TIMESTAMP;

            if (percentageUsage < 0)
                percentageUsage = 0;
            else if (percentageUsage > 100)
                percentageUsage = 100;

            if (cpuSecCount >= 6 || monitor.cpuFirstData) {
                cpuDataMin = sliceFirstData(cpuDataMin, dataView.Minutes);
                cpuDataMin.push({ "x": new Date(timeStamp), "y": percentageUsage });
                MonitorGraphUI.Monitors.cpuDataMin = cpuDataMin;
                cpuSecCount = 0;
            }
            if (cpuMinCount >= 60 || monitor.cpuFirstData) {
                cpuDataDay = sliceFirstData(cpuDataDay, dataView.Days);
                cpuDataDay.push({ "x": new Date(timeStamp), "y": percentageUsage });
                MonitorGraphUI.Monitors.cpuDataHrs = cpuDataDay;
                cpuMinCount = 0;
            }
            cpuData = sliceFirstData(cpuData, dataView.Seconds);
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

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && cpuChart.is(":visible")) {
                d3.select('#visualisationCpu')
                    .datum(dataCpu)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCpu);
            }
            cpuSecCount++;
            cpuMinCount++;
        };

        function getPartitionData() {
            var monitor = MonitorGraphUI.Monitors;
            monitor.partitionData = getEmptyDataForPartition();
            monitor.partitionDataMin = getEmptyDataForPartitionForMinutes();
            monitor.partitionDataDay = getEmptyDataForPartitionForDay();
        }

        this.RefreshPartitionIdleTime = function (partitionDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            if (monitor.partitionData.length < 1 || monitor.partitionDataMin.length < 1 || monitor.partitionDataDay.length < 1) {
                getPartitionData();
            }
            var partitionData = monitor.partitionData;
            var partitionDataMin = monitor.partitionDataMin;
            var partitionDataDay = monitor.partitionDataDay;
            var partitionDetail = partitionDetails;

            if ($.isEmptyObject(partitionDetail) || partitionDetail == undefined ||partitionDetail["partitionDetail"]["timeStamp"] == undefined)
                return;

            var timeStamp = partitionDetails["partitionDetail"]["timeStamp"];
            $.each(partitionDetail["partitionDetail"], function (datatype, datavalue) {
                $.each(datavalue, function (partitionKey, partitionValue) {
                    var keyValue = partitionKey;
                    var percentValue = partitionValue;

                    if (percentValue < 0)
                        percentValue = 0;
                    else if (percentValue > 100)
                        percentValue = 100;

                    if (partitionSecCount >= 6 || monitor.partitionFirstData) {
                        if (!partitionDataMin.hasOwnProperty(keyValue)) {
                            var keyIndex = dataMapperMin[keyValue];
                            partitionDataMin[keyIndex]["values"] = sliceFirstData(partitionDataMin[keyIndex]["values"], dataView.Minutes);
                            partitionDataMin[keyIndex]["values"].push({ 'x': new Date(timeStamp), 'y': percentValue });
                            MonitorGraphUI.Monitors.partitionDataMin = partitionDataMin;
                        }
                    }

                    if (partitionMinCount >= 60 || monitor.partitionFirstData) {
                        var keyIndexDay = dataMapperDay[keyValue];
                        partitionDataDay[keyIndexDay]["values"] = sliceFirstData(partitionDataDay[keyIndexDay]["values"], dataView.Days);
                        partitionDataDay[keyIndexDay]["values"].push({ 'x': new Date(timeStamp), 'y': percentValue });
                        MonitorGraphUI.Monitors.partitionDataDay = partitionDataDay;
                    }
                    var keyIndexSec = dataMapperSec[keyValue];
                    partitionData[keyIndexSec]["values"] = sliceFirstData(partitionData[keyIndexSec]["values"], dataView.Seconds);
                    partitionData[keyIndexSec].values.push({ 'x': new Date(timeStamp), 'y': percentValue });
                    MonitorGraphUI.Monitors.partitionData = partitionData;
                });
            });
            if (monitor.partitionFirstData) {
                $(".legend").css("display", "block");
            }
            monitor.partitionFirstData = false;
            if (partitionSecCount >= 6)
                partitionSecCount = 0;
            if (partitionMinCount >= 60)
                partitionMinCount = 0;

            if (graphView == 'Minutes')
                dataPartitionIdleTime = partitionDataMin;
            else if (graphView == 'Days')
                dataPartitionIdleTime = partitionDataDay;
            else {
                dataPartitionIdleTime = partitionData;
            }

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && partitionChart.is(":visible")) {
                d3.select('#visualisationPartitionIdleTime')
                    .datum(dataPartitionIdleTime)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartPartitionIdleTime);
            }

            partitionSecCount++;
            partitionMinCount++;
        };

        this.RefreshDrReplicationGraph = function (drDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var drData = monitor.drReplicationData;
            var drDataMin = monitor.drReplicationDataMin;
            var drDataDay = monitor.drReplicationDataDay;
            var drDetail = drDetails;

            if ($.isEmptyObject(drDetail) || drDetail == undefined || drDetail["DR_GRAPH"].REPLICATION_RATE_1M == undefined || drDetail["DR_GRAPH"].TIMESTAMP == undefined)
                return;

            var plottingPoint = parseFloat(drDetail["DR_GRAPH"].REPLICATION_RATE_1M).toFixed(1) * 1;
            var timeStamp = drDetail["DR_GRAPH"].TIMESTAMP;

            if (drSecCount >= 6 || monitor.drFirstData) {
                drDataMin = sliceFirstData(drDataMin, dataView.Minutes);
                drDataMin.push({ "x": new Date(timeStamp), "y": plottingPoint });
                MonitorGraphUI.Monitors.drReplicationDataMin = drDataMin;
                drSecCount = 0;
            }
            if (drMinCount >= 60 || monitor.drFirstData) {
                drDataDay = sliceFirstData(drDataDay, dataView.Days);
                drDataDay.push({ "x": new Date(timeStamp), "y": plottingPoint });
                MonitorGraphUI.Monitors.drReplicationDataDay = drDataDay;
                drMinCount = 0;
            }
            drData = sliceFirstData(drData, dataView.Seconds);
            drData.push({ "x": new Date(timeStamp), "y": plottingPoint });
            MonitorGraphUI.Monitors.drReplicationData = drData;
            monitor.drFirstData = false;

            if (graphView == 'Minutes')
                dataDrReplicationRate[0]["values"] = drDataMin;
            else if (graphView == 'Days')
                dataDrReplicationRate[0]["values"] = drDataDay;
            else {
                dataDrReplicationRate[0]["values"] = drData;

            }

            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && drReplicationChart.is(":visible")) {
                d3.select('#visualizationDrReplicationRate')
                    .datum(dataDrReplicationRate)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartDrReplicationRate);
            }
            drSecCount++;
            drMinCount++;
        };

        this.RefreshCommandLog = function (cmdLogDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var cmdLogData = monitor.cmdLogData;
            var cmdLogDataMin = monitor.cmdLogDataMin;
            var cmdLogDataDay = monitor.cmdLogDataDay;
            var cmdLogDetail = cmdLogDetails;

            if ($.isEmptyObject(cmdLogDetail) || cmdLogDetail == undefined || cmdLogDetail[currentServer].OUTSTANDING_TXNS == undefined || cmdLogDetail[currentServer].TIMESTAMP == undefined)
                return;

            var outStandingTxn = parseFloat(cmdLogDetail[currentServer].OUTSTANDING_TXNS).toFixed(1) * 1;
            var timeStamp = cmdLogDetail[currentServer].TIMESTAMP;

            if (cmdLogSecCount >= 6 || monitor.cmdLogFirstData) {
                cmdLogDataMin = sliceFirstData(cmdLogDataMin, dataView.Minutes);
                cmdLogDataMin.push({ "x": new Date(timeStamp), "y": outStandingTxn });
                MonitorGraphUI.Monitors.cmdLogDataMin = cmdLogDataMin;
                cmdLogSecCount = 0;
            }
            if (cmdLogMinCount >= 60 || monitor.cmdLogFirstData) {
                cmdLogDataDay = sliceFirstData(cmdLogDataDay, dataView.Days);
                cmdLogDataDay.push({ "x": new Date(timeStamp), "y": outStandingTxn });
                MonitorGraphUI.Monitors.cmdLogDataDay = cmdLogDataDay;
                cmdLogMinCount = 0;
            }
            cmdLogData = sliceFirstData(cmdLogData, dataView.Seconds);
            cmdLogData.push({ "x": new Date(timeStamp), "y": outStandingTxn });
            MonitorGraphUI.Monitors.cmdLogData = cmdLogData;
            if (monitor.cmdLogFirstData) {
                $(".cmdLogLegend").css("display", "block");
            }
            monitor.cmdLogFirstData = false;

            if (graphView == 'Minutes')
                dataCommandLog[0]["values"] = cmdLogDataMin;
            else if (graphView == 'Days')
                dataCommandLog[0]["values"] = cmdLogDataDay;
            else {
                dataCommandLog[0]["values"] = cmdLogData;
            }


            if (currentTab == NavigationTabs.DBMonitor && currentView == graphView && cmdLogChart.is(":visible")) {
                d3.select('#visualisationCommandLog')
                    .datum(dataCommandLog)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCommandlog);
            }

            var isDuplicate = false;
            if (!$.isEmptyObject(cmdLogDetail[currentServer].SNAPSHOTS)) {
                for (var i = 0; i < cmdLogDetail[currentServer].SNAPSHOTS.length; i++) {
                    $.each(cmdLogOverlay, function (partitionKey, partitionValue) {
                        var x1 = partitionValue.x;
                        if (x1 == cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME)
                            isDuplicate = true;
                        else
                            isDuplicate = false;
                    });
                    if (!isDuplicate)
                        cmdLogOverlay.push({ "x": cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME, "y": cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME });
                }
            }
            d3.select('#visualisationCommandLog .nv-y')
                .append('rect')
                .attr('x', 2)
                .attr('width', 560)
                .style('fill', 'white')
                .style('opacity', 1)
                .attr('y', 0)
                .attr('height', MonitorGraphUI.ChartCommandlog.yAxis.range()[0]);

            $.each(cmdLogOverlay, function (partitionKey, partitionValue) {
                var x1 = MonitorGraphUI.ChartCommandlog.xScale()(partitionValue.x);
                var x2 = MonitorGraphUI.ChartCommandlog.xScale()(partitionValue.y);
                var opacity = 1;
                if (x1 > 3 && x1 < 560 && (x2 - x1 > 0)) {
                    opacity = ((x2 - x1) > 4) ? 0.2 : 1;
                    d3.select('#visualisationCommandLog .nv-y')
                        .append('rect')
                        .attr('x', x1)
                        .attr('width', (x2 - x1))
                        .style('fill', 'red')
                        .style('opacity', opacity)
                        .attr('y', 0)
                        .attr('height', MonitorGraphUI.ChartCommandlog.yAxis.range()[0]);
                }
            });

            cmdLogSecCount++;
            cmdLogMinCount++;
        };

        this.refreshGraphCmdLog = function () {
            if ($.isFunction(MonitorGraphUI.ChartCommandlog.update))
                MonitorGraphUI.ChartCommandlog.update();
        };

        this.refreshGraphDR = function () {
            if ($.isFunction(MonitorGraphUI.ChartDrReplicationRate.update))
                MonitorGraphUI.ChartDrReplicationRate.update();
        };
    });



    window.MonitorGraphUI = MonitorGraphUI = new IMonitorGraphUI();
})(window);

