
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
        var drReplicationChart;        var cmdLogChart;        var cmdLogOverlay = [];        var physicalMemory = -1;
        this.Monitors = {};
        this.ChartCpu = nv.models.lineChart();
        this.ChartRam = nv.models.lineChart();
        this.ChartLatency = nv.models.lineChart();
        this.ChartTransactions = nv.models.lineChart();
        this.ChartPartitionIdleTime = nv.models.lineChart();
        this.ChartDrReplicationRate = nv.models.lineChart();        this.ChartCommandlog = nv.models.lineChart();        var dataMapperSec = {};
        var dataMapperMin = {};
        var dataMapperDay = {};
        this.enumPartitionColor = {
            localPartition: "#D3D3D3",
            maxMinPartition: "#4C76B0",
            multiPartition: "#FF8C00"
        }
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
                                dataPartition.push({ key: partitionKey, values: arr, color: MonitorGraphUI.enumPartitionColor.localPartition });
                            } else if (datatype == "dataMPI") {
                                dataPartition.push({ key: partitionKey, values: arr, color: MonitorGraphUI.enumPartitionColor.multiPartition });
                            } else if (datatype == "dataMax" || datatype == "dataMin") {
                                dataPartition.push({ key: partitionKey, values: arr, color: MonitorGraphUI.enumPartitionColor.maxMinPartition});
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
                
                MonitorGraphUI.ChartCpu.margin({ left: 100 });
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
                MonitorGraphUI.ChartCpu.useInteractiveGuideline(true);
                //var tooltip = MonitorGraphUI.ChartCpu.tooltip;
                //tooltip.gravity('s');
                //tooltip.contentGenerator(function (d) {
                //    var html = '';
                //    d.series.forEach(function (elem) {
                //        html += "<h3>"
                //            + elem.key + "</h3>";
                //    });
                //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + "% at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                //    return html;
                //});
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

                MonitorGraphUI.ChartRam.margin({ left: 100 });
                MonitorGraphUI.ChartRam.lines.forceY([0, 0.1]);

                d3.select('#visualisationRam')
                    .datum(dataRam)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartRam);

                nv.utils.windowResize(MonitorGraphUI.ChartRam.update);
            },
            callback: function (p) {
                MonitorGraphUI.ChartRam.useInteractiveGuideline(true);
                //var tooltip = MonitorGraphUI.ChartRam.tooltip;

                //tooltip.contentGenerator(function (d) {
                //    var html = '';
                //    d.series.forEach(function (elem) {
                //        html += "<h3>"
                //            + elem.key + "</h3>";
                //    });
                //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(4) + " GB at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                //    return html;
                //});

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

                MonitorGraphUI.ChartLatency.margin({ left: 100 });
                MonitorGraphUI.ChartLatency.lines.forceY([0, 1]);

                d3.select('#visualisationLatency')
                    .datum(dataLatency)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartLatency);

                nv.utils.windowResize(MonitorGraphUI.ChartLatency.update);
            },
            callback: function(p) {
                MonitorGraphUI.ChartLatency.useInteractiveGuideline(true);
                //var tooltip = MonitorGraphUI.ChartLatency.tooltip;
               
                //tooltip.contentGenerator(function (d) {
                //    var html = '';
                //    d.series.forEach(function (elem) {
                //        html += "<h3>"
                //            + elem.key + "</h3>";
                //    });
                //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " ms at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

                //    //d.series.forEach(function (elem) {
                //    //    html += "<table><tr><td colspan='3'><strong class='x=value'>" + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</strong></td></tr></thead>" +
                //    //        "<tbody><tr><td class='legend-color-guide'><div style='background-color: rgb(27,135,200);'</div></td><td class='key'>Latency</td><td class='value'>" + parseFloat(d.point.y).toFixed(2) + "</td></tr></tbody>";
                //    //});
    
                //    return html;
                //});
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

               MonitorGraphUI.ChartTransactions.margin({ left: 100 });
               MonitorGraphUI.ChartTransactions.lines.forceY([0, 1]);

               d3.select('#visualisationTransaction')
                   .datum(dataTransactions)
                   .transition().duration(500)
                   .call(MonitorGraphUI.ChartTransactions);

               nv.utils.windowResize(MonitorGraphUI.ChartTransactions.update);
           },
           callback: function(p) {
               MonitorGraphUI.ChartTransactions.useInteractiveGuideline(true);
               //var tooltip = MonitorGraphUI.ChartTransactions.tooltip;
               //tooltip.contentGenerator(function (d) {
               //    var html = '';
               //    d.series.forEach(function (elem) {
               //        html += "<h3>"
               //            + elem.key + "</h3>";
               //    });
               //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " tps at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

               //    return html;
               //});
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

                MonitorGraphUI.ChartPartitionIdleTime.margin({ left: 100 });
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

                MonitorGraphUI.ChartDrReplicationRate.margin({ left: 100 });
                MonitorGraphUI.ChartDrReplicationRate.lines.forceY([0, 1]);

                d3.select('#visualizationDrReplicationRate')
                    .datum(dataDrReplicationRate)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartDrReplicationRate);

                nv.utils.windowResize(MonitorGraphUI.ChartDrReplicationRate.update);
            },
            callback: function() {
                MonitorGraphUI.ChartDrReplicationRate.useInteractiveGuideline(true);
                //var tooltip = MonitorGraphUI.ChartDrReplicationRate.tooltip;
                //tooltip.contentGenerator(function (d) {
                //    debugger;
                //    var html = '';
                //    d.series.forEach(function (elem) {
                //        html += "<h3>"
                //            + elem.key + "</h3>";
                //    });
                //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " KBps at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";
                //    return html;
                //});
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

                MonitorGraphUI.ChartCommandlog.margin({ left: 100 });
                MonitorGraphUI.ChartCommandlog.lines.forceY([0, 0.1]);

                d3.select('#visualisationCommandLog')
                    .datum(dataCommandLog)
                    .transition().duration(500)
                    .call(MonitorGraphUI.ChartCommandlog);

                nv.utils.windowResize(MonitorGraphUI.ChartCommandlog.update);
           },
            callback:function() {
               MonitorGraphUI.ChartCommandlog.useInteractiveGuideline(true);
               //var tooltip = MonitorGraphUI.ChartCommandlog.tooltip;
               //tooltip.contentGenerator(function (d) {
               //    var html = '';
               //    d.series.forEach(function (elem) {
               //        html += "<h3>"
               //            + elem.key + "</h3>";
               //    });
               //    html = html + "<h2>" + parseFloat(d.point.y).toFixed(2) + " Pending at " + d3.time.format('%d %b %X')(new Date(d.point.x)) + "</h2>";

               //    return html;
               //});
               return MonitorGraphUI.ChartCommandlog;
           }
        });

        goog.math.Long.prototype.numberOfLeadingZeros = function () {
            var n = 1;
            var x = this.high_;
            if (x == 0) { n += 32; x = this.low_; }
            if (x >>> 16 == 0) { n += 16; x <<= 16; }
            if (x >>> 24 == 0) { n +=  8; x <<=  8; }
            if (x >>> 28 == 0) { n +=  4; x <<=  4; }
            if (x >>> 30 == 0) { n +=  2; x <<=  2; }
            n -= x >>> 31;
            return n;
        };


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
            var subBucketMask = goog.math.Long.fromInt(this.subBucketCount - 1);
            this.subBucketMask = subBucketMask.shiftLeft(this.unitMagnitude);
            // Establish leadingZeroCountBase, used in getBucketIndex() fast path:
            this.leadingZeroCountBase = 64 - this.unitMagnitude - this.subBucketHalfCountMagnitude - 1;
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

        Histogram.prototype.normalizeIndex = function (index, normalizingIndexOffset, arrayLength) {
            if (normalizingIndexOffset == 0) {
                // Fastpath out of normalization. Keeps integer value histograms fast while allowing
                // others (like DoubleHistogram) to use normalization at a cost...
                return index;
            }
            if ((index > arrayLength) || (index < 0)) {
                throw new ArrayIndexOutOfBoundsException("index out of covered value range");
            }
            var normalizedIndex = index - normalizingIndexOffset;
            // The following is the same as an unsigned remainder operation, as long as no double wrapping happens
            // (which shouldn't happen, as normalization is never supposed to wrap, since it would have overflowed
            // or underflowed before it did). This (the + and - tests) seems to be faster than a % op with a
            // correcting if < 0...:
            if (normalizedIndex < 0) {
                normalizedIndex += arrayLength;
            } else if (normalizedIndex >= arrayLength) {
                normalizedIndex -= arrayLength;
            }
            return normalizedIndex;
        };

        Histogram.prototype.getCountAtIndex = function (index) {
            return this.count[this.normalizeIndex(index, 0, this.countsArrayLength)];
        };

        Histogram.prototype.valueFromIndex2 = function (bucketIndex, subBucketIndex) {
            return subBucketIndex * Math.pow(2, bucketIndex + this.unitMagnitude);
        };

        Histogram.prototype.valueFromIndex = function (index) {
            var bucketIndex = (index >> this.subBucketHalfCountMagnitude) - 1;
            var subBucketIndex = (index & (this.subBucketHalfCount - 1)) + this.subBucketHalfCount;
            if (bucketIndex < 0) {
                subBucketIndex -= this.subBucketHalfCount;
                bucketIndex = 0;
            }
            return this.valueFromIndex2(bucketIndex, subBucketIndex);
        };

        Histogram.prototype.lowestEquivalentValue = function (value) {
            var bucketIndex = this.getBucketIndex(value);
            var subBucketIndex = this.getSubBucketIndex(value, bucketIndex);
            var thisValueBaseLevel = this.valueFromIndex2(bucketIndex, subBucketIndex);
            return thisValueBaseLevel;
        };

        Histogram.prototype.highestEquivalentValue = function (value) {
            return this.nextNonEquivalentValue(value) - 1;
        };

        Histogram.prototype.highestEquivalentValue = function (value) {
            return this.lowestEquivalentValue(value) + this.sizeOfEquivalentValueRange(value);
        };

        Histogram.prototype.sizeOfEquivalentValueRange = function (value) {
            var bucketIndex = this.getBucketIndex(value);
            var subBucketIndex = this.getSubBucketIndex(value, bucketIndex);
            var distanceToNextValue =
                (1 << ( this.unitMagnitude + ((subBucketIndex >= this.subBucketCount) ? (bucketIndex + 1) : bucketIndex)));
            return distanceToNextValue;
        };

        Histogram.prototype.getBucketIndex = function (value) {
            return this.leadingZeroCountBase - (goog.math.Long.fromNumber(value).or(this.subBucketMask)).numberOfLeadingZeros();
        };

        Histogram.prototype.getSubBucketIndex = function (value, bucketIndex) {
            return  (value >>> (bucketIndex + this.unitMagnitude));
        };

        Histogram.prototype.getValueAtPercentile = function (percentile) {
            var requestedPercentile = Math.min(percentile, 100.0); // Truncate down to 100%
            var countAtPercentile = Math.floor(((percentile / 100.0) * this.totalCount) + 0.5); // round to nearest
            countAtPercentile = Math.max(countAtPercentile, 1); // Make sure we at least reach the first recorded entry
            var totalToCurrentIndex = 0;
            for (var i = 0; i < this.countsArrayLength; i++) {
                totalToCurrentIndex += this.getCountAtIndex(i);
                if (totalToCurrentIndex >= countAtPercentile) {
                    var valueAtIndex = this.valueFromIndex(i);
                    return (percentile == 0.0) ?
                        this.lowestEquivalentValue(valueAtIndex)/1000.0 :
                        this.highestEquivalentValue(valueAtIndex)/1000.0;
                }
            }
            return 0;
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
            cmdLogChart = cmdLogChartObj;            currentView = view;
            MonitorGraphUI.Monitors = {
                'latHistogram': {},
                'latData': getEmptyDataOptimized(),
                'latDataMin': getEmptyDataForMinutesOptimized(),
                'latDataDay': getEmptyDataForDaysOptimized(),
                'latFirstData': true,
                'latMaxTimeStamp': null,
                'tpsData': getEmptyDataOptimized(),
                'tpsDataMin': getEmptyDataForMinutesOptimized(),
                'tpsDataDay': getEmptyDataForDaysOptimized(),
                'tpsFirstData': true,
                'memData': getEmptyDataOptimized(),
                'memDataMin': getEmptyDataForMinutesOptimized(),
                'memDataDay': getEmptyDataForDaysOptimized(),
                'memFirstData': true,
                'memMaxTimeStamp': null,
                'cpuData': getEmptyDataOptimized(),
                'cpuDataMin': getEmptyDataForMinutesOptimized(),
                'cpuDataHrs': getEmptyDataForDaysOptimized(),
                'cpuFirstData': true,
                'cpuMaxTimeStamp':null,
                'partitionData': getEmptyDataForPartition(),
                'partitionDataMin': getEmptyDataForPartitionForMinutes(),
                'partitionDataDay': getEmptyDataForPartitionForDay(),
                'partitionFirstData': true,
                'partitionMaxTimeStamp':null,
                'drReplicationData': getEmptyDataOptimized(),
                'drReplicationDataMin': getEmptyDataForMinutesOptimized(),
                'drReplicationDataDay': getEmptyDataForDaysOptimized(),
                'drMaxTimeStamp': null,
                'cmdLogData': getEmptyDataOptimized(),
                'cmdLogDataMin': getEmptyDataForMinutesOptimized(),
                'cmdLogDataDay': getEmptyDataForDaysOptimized(),
                'cmdLogFirstData': true,
                'cmdLogMaxTimeStamp': null,
                'drFirstData': true,
                'lastTimedTransactionCount': -1,
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

            if (drReplicationChart.is(":visible"))                MonitorGraphUI.ChartDrReplicationRate.update();

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
            var latencyArr = []
            var latencyArrMin = []
            var latencyArrDay = []

            if(localStorage.latencyMin != undefined)
                latencyArrMin = JSON.parse(localStorage.latencyMin)

            if(localStorage.latency != undefined)
                latencyArr = JSON.parse(localStorage.latency)

            if(localStorage.latencyDay != undefined)
                latencyArrDay = JSON.parse(localStorage.latencyDay)

            if(monitor.latFirstData){
                for(var i = 0; i< latencyArr.length; i++){
                    sliceFirstData(monitor.latData, dataView.Seconds);
                    monitor.latData.push({"x": new Date(latencyArr[i].timestamp),
                        "y": latencyArr[i].latency
                    })
                }
                 for(var j = 0; j< latencyArrMin.length; j++){
                    sliceFirstData(monitor.latDataMin, dataView.Minutes);
                    monitor.latDataMin.push({"x": new Date(latencyArrMin[j].timestamp),
                        "y": latencyArrMin[j].latency
                    })
                }
            }

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
            if (monitor.latMaxTimeStamp <= timeStamp) {
                if (latSecCount >= 6 || monitor.latFirstData) {
                    dataLatMin = sliceFirstData(dataLatMin, dataView.Minutes);
                    if (monitor.latMaxTimeStamp == timeStamp) {
                        dataLatMin.push({ 'x': new Date(timeStamp), 'y': dataLatMin[dataLatMin.length - 1].y });
                        latencyArrMin = MonitorGraphUI.saveLocalStorage(latencyArrMin, {"timestamp": new Date(timeStamp), "latency": dataLatMin[dataLatMin.length - 1].y }, MonitorGraphUI.timeUnit.min  )
                    } else {
                        dataLatMin.push({ 'x': new Date(timeStamp), 'y': lat });
                        latencyArrMin = MonitorGraphUI.saveLocalStorage(latencyArrMin, {"timestamp": new Date(timeStamp), "latency": lat }, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.latDataMin = dataLatMin;
                    latSecCount = 0;
                }

                if (latMinCount >= 60 || monitor.latFirstData) {
                    dataLatDay = sliceFirstData(dataLatDay, dataView.Days);
                    if (monitor.latMaxTimeStamp == timeStamp) {
                        dataLatDay.push({ 'x': new Date(timeStamp), 'y': dataLatDay[dataLatDay.length - 1].y });
                        latencyArrDay = MonitorGraphUI.saveLocalStorage(latencyArrDay, {"timestamp": new Date(timeStamp), "latency": dataLatMin[dataLatMin.length - 1].y }, MonitorGraphUI.timeUnit.day  )
                    } else {
                        dataLatDay.push({ 'x': new Date(timeStamp), 'y': lat });
                        latencyArrDay = MonitorGraphUI.saveLocalStorage(latencyArrDay, {"timestamp": new Date(timeStamp), "latency": lat }, MonitorGraphUI.timeUnit.day  )
                    }
                    MonitorGraphUI.Monitors.latDataDay = dataLatDay;
                    latMinCount = 0;
                }

                dataLat = sliceFirstData(dataLat, dataView.Seconds);
                if (monitor.latMaxTimeStamp == timeStamp) {
                    dataLat.push({ 'x': new Date(timeStamp), 'y': dataLat[dataLat.length - 1].y });
                    latencyArr = MonitorGraphUI.saveLocalStorage(latencyArr, {"timestamp": new Date(timeStamp), "latency": dataLat[dataLat.length - 1].y }, MonitorGraphUI.timeUnit.sec  )
                } else {
                    dataLat.push({ 'x': new Date(timeStamp), 'y': lat });
                    latencyArr = MonitorGraphUI.saveLocalStorage(latencyArr, {"timestamp": new Date(timeStamp), "latency": lat }, MonitorGraphUI.timeUnit.sec  )
                }
                MonitorGraphUI.Monitors.latData = dataLat;

                localStorage.latency = JSON.stringify(latencyArr)
                localStorage.latencyMin = JSON.stringify(latencyArrMin)
                localStorage.latencyDay = JSON.stringify(latencyArrDay)

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
            }
            if(timeStamp > monitor.latMaxTimeStamp)
                monitor.latMaxTimeStamp = timeStamp;
            latSecCount++;
            latMinCount++;
        };

        this.RefreshMemory = function (memoryDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var dataMem = monitor.memData;
            var dataMemMin = monitor.memDataMin;
            var dataMemDay = monitor.memDataDay;
            var memDetails = memoryDetails;
            var x = 0;
            var y = 0;
            var memoryDetailsArr = []
            var memoryDetailsArrMin = []
            var memoryDetailsArrDay = []

            if(localStorage.memoryDetailsMin != undefined)
                memoryDetailsArrMin = JSON.parse(localStorage.memoryDetailsMin)
            else {
                memoryDetailsArrMin =  JSON.stringify(MonitorGraphUI.convertDataFormatForMemory(dataMemMin))
                memoryDetailsArrMin = JSON.parse(memoryDetailsArrMin)
            }


            if(localStorage.memoryDetails != undefined)
                memoryDetailsArr = JSON.parse(localStorage.memoryDetails)
            else {
                memoryDetailsArr = JSON.stringify(MonitorGraphUI.convertDataFormatForMemory(dataMem))
                memoryDetailsArr = JSON.parse(memoryDetailsArr)
            }

            if(localStorage.memoryDetailsDay != undefined)
                memoryDetailsArrDay = JSON.parse(localStorage.memoryDetailsDay)
            else {
                memoryDetailsArrDay = JSON.stringify(MonitorGraphUI.convertDataFormatForMemory(dataMemDay))
                memoryDetailsArrDay = JSON.parse(memoryDetailsArrDay)
            }

            if(monitor.memFirstData){
                if(memoryDetailsArr.length != 0)
                    dataMem = []
                for(var i = 0; i< memoryDetailsArr.length; i++){
                    sliceFirstData(monitor.memData, dataView.Seconds);
                    dataMem.push({"x": new Date(memoryDetailsArr[i].timestamp),
                        "y": memoryDetailsArr[i].physicalMemory
                    })
                }

                if(memoryDetailsArrMin.length != 0)
                    dataMemMin = []
                for(var j = 0; j< memoryDetailsArrMin.length; j++){
                    sliceFirstData(monitor.memDataMin, dataView.Minutes);
                    dataMemMin.push({"x": new Date(memoryDetailsArrMin[j].timestamp),
                        "y": memoryDetailsArrMin[j].physicalMemory
                    })
                }

                if(memoryDetailsArrDay.length != 0)
                    dataMemDay = []
                for(var k = 0; k< memoryDetailsArrDay.length; k++){
                    sliceFirstData(monitor.memDataDay, dataView.Days);
                    dataMemDay.push({"x": new Date(memoryDetailsArrDay[k].timestamp),
                        "y": memoryDetailsArrDay[k].physicalMemory
                    })
                }
            }

            if ($.isEmptyObject(memDetails) || memDetails == undefined || memDetails[currentServer].PHYSICALMEMORY == undefined || memDetails[currentServer].RSS == undefined || memDetails[currentServer].TIMESTAMP == undefined)
                return;
            var memTimeStamp = new Date(memDetails[currentServer].TIMESTAMP);

            if (memTimeStamp >= monitor.memMaxTimeStamp) {
                var memRss = parseFloat(memDetails[currentServer].RSS * 1.0 / 1048576.0).toFixed(3) * 1;

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
                    if (memTimeStamp == monitor.memMaxTimeStamp) {
                        dataMemMin.push({ "x": new Date(memTimeStamp), "y": dataMemMin[dataMemMin.length - 1].y });
                        memoryDetailsArrMin = MonitorGraphUI.saveLocalStorage(memoryDetailsArrMin, {"timestamp": new Date(memTimeStamp), "physicalMemory": dataMemMin[dataMemMin.length - 1].y }, MonitorGraphUI.timeUnit.min  )
                    } else {
                        dataMemMin.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                        memoryDetailsArrMin = MonitorGraphUI.saveLocalStorage(memoryDetailsArrMin, {"timestamp": new Date(memTimeStamp), "physicalMemory": memRss }, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.memDataMin = dataMemMin;
                    memSecCount = 0;
                }

                if (memMinCount >= 60 || monitor.memFirstData) {
                    dataMemDay = sliceFirstData(dataMemDay, dataView.Days);
                    if (memTimeStamp == monitor.memMaxTimeStamp) {
                        dataMemDay.push({ "x": new Date(memTimeStamp), "y": dataMemDay[dataMemDay.length - 1].y });
                        memoryDetailsArrDay = MonitorGraphUI.saveLocalStorage(memoryDetailsArrDay, {"timestamp": new Date(memTimeStamp), "physicalMemory": dataMemDay[dataMemDay.length - 1].y}, MonitorGraphUI.timeUnit.day )
                    } else {
                        dataMemDay.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                        memoryDetailsArrDay = MonitorGraphUI.saveLocalStorage(memoryDetailsArrDay, {"timestamp": new Date(memTimeStamp), "physicalMemory": memRss}, MonitorGraphUI.timeUnit.day )
                    }
                    MonitorGraphUI.Monitors.memDataDay = dataMemDay;
                    memMinCount = 0;
                }

                dataMem = sliceFirstData(dataMem, dataView.Seconds);
                if (memTimeStamp == monitor.memMaxTimeStamp) {
                    dataMem.push({ "x": new Date(memTimeStamp), "y": dataMem[dataMem.length - 1].y });
                    memoryDetailsArr = MonitorGraphUI.saveLocalStorage(memoryDetailsArr, {"timestamp": new Date(memTimeStamp), "physicalMemory": dataMem[dataMem.length - 1].y}, MonitorGraphUI.timeUnit.sec  )
                } else {
                    dataMem.push({ 'x': new Date(memTimeStamp), 'y': memRss });
                    memoryDetailsArr = MonitorGraphUI.saveLocalStorage(memoryDetailsArr, {"timestamp": new Date(memTimeStamp), "physicalMemory": memRss}, MonitorGraphUI.timeUnit.sec  )
                }
                MonitorGraphUI.Monitors.memData = dataMem;


                localStorage.memoryDetails = JSON.stringify(memoryDetailsArr)
                localStorage.memoryDetailsMin = JSON.stringify(memoryDetailsArrMin)
                localStorage.memoryDetailsDay = JSON.stringify(memoryDetailsArrDay)

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
            }
            if (memTimeStamp > monitor.memMaxTimeStamp)
                monitor.memMaxTimeStamp = memTimeStamp;
            memSecCount++;
            memMinCount++;
        };

        this.RefreshTransaction = function (transactionDetails, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var datatrans = monitor.tpsData;
            var datatransMin = monitor.tpsDataMin;
            var datatransDay = monitor.tpsDataDay;
            var transacDetail = transactionDetails;
            var transDetailsArr = [];
            var transDetailsArrMin = [];
            var transDetailsArrDay = [];

            if(localStorage.transDetailsMin != undefined)
                transDetailsArrMin = JSON.parse(localStorage.transDetailsMin)

            if(localStorage.transDetails != undefined)
                transDetailsArr = JSON.parse(localStorage.transDetails)

            if(localStorage.transDetailsDay != undefined)
                transDetailsArrDay = JSON.parse(localStorage.transDetailsDay)

            if(monitor.tpsFirstData){
                for(var i = 0; i< transDetailsArr.length; i++){
                    sliceFirstData(monitor.tpsData, dataView.Seconds);
                    monitor.tpsData.push({"x": new Date(transDetailsArr[i].timestamp),
                        "y": transDetailsArr[i].transaction
                    })
                }
                for(var j = 0; j< transDetailsArrMin.length; j++){
                    sliceFirstData(monitor.tpsDataMin, dataView.Minutes);
                    monitor.tpsDataMin.push({"x": new Date(transDetailsArrMin[j].timestamp),
                        "y": transDetailsArrMin[j].transaction
                    })
                }
                for(var k = 0; k< transDetailsArrDay.length; k++){
                    sliceFirstData(monitor.tpsDataDay, dataView.Day);
                    monitor.tpsDataDay.push({"x": new Date(transDetailsArrDay[k].timestamp),
                        "y": transDetailsArrDay[k].transaction
                    })
                }
            }

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
                    if (monitor.tpsFirstData || delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0) || calculatedValue == 0) {
                        datatransMin.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                        transDetailsArrMin = MonitorGraphUI.saveLocalStorage(transDetailsArrMin, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": calculatedValue }, MonitorGraphUI.timeUnit.min  )
                    } else {
                        datatransMin.push({ "x": new Date(transacDetail["TimeStamp"]), "y": datatransMin[datatransMin.length - 1].y });
                        transDetailsArrMin = MonitorGraphUI.saveLocalStorage(transDetailsArrMin, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": datatransMin[datatransMin.length - 1].y }, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.tpsDataMin = datatransMin;
                    tpsSecCount = 0;
                }
                if (tpsMinCount >= 60 || monitor.tpsFirstData) {
                    datatransDay = sliceFirstData(datatransDay, dataView.Days);
                    if (monitor.tpsFirstData || delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)|| calculatedValue == 0) {
                        datatransDay.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                        transDetailsArrDay = MonitorGraphUI.saveLocalStorage(transDetailsArrDay, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": calculatedValue }, MonitorGraphUI.timeUnit.day  )
                    } else {
                        datatransDay.push({ "x": new Date(transacDetail["TimeStamp"]), "y": datatransDay[datatransDay.length - 1].y });
                        transDetailsArrDay = MonitorGraphUI.saveLocalStorage(transDetailsArrDay, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": datatransDay[datatransDay.length - 1].y }, MonitorGraphUI.timeUnit.day  )
                    }
                    MonitorGraphUI.Monitors.tpsDataDay = datatransDay;
                    tpsMinCount = 0;
                }
                datatrans = sliceFirstData(datatrans, dataView.Seconds);
                if (monitor.tpsFirstData || delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)|| calculatedValue == 0) {
                    datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                    transDetailsArr = MonitorGraphUI.saveLocalStorage(transDetailsArr, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": calculatedValue }, MonitorGraphUI.timeUnit.sec  )
                } else {
                    datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": datatrans[datatrans.length - 1].y });
                    transDetailsArr = MonitorGraphUI.saveLocalStorage(transDetailsArr, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": datatrans[datatrans.length - 1].y }, MonitorGraphUI.timeUnit.sec  )
                }
                MonitorGraphUI.Monitors.tpsData = datatrans;
                monitor.tpsFirstData = false;
            }
            else{
                var delta = currentTimedTransactionCount - monitor.lastTimedTransactionCount;

                if (tpsSecCount >= 6 || monitor.tpsFirstData) {
                    datatransMin = sliceFirstData(datatransMin, dataView.Minutes);
                    if (monitor.tpsFirstData || delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)) {
                        transDetailsMin = MonitorGraphUI.saveLocalStorage(transDetailsArrMin, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": 0 }, MonitorGraphUI.timeUnit.Min  )
                        datatransMin.push({ "x": new Date(transacDetail["TimeStamp"]), "y": 0 });
                    }
                    MonitorGraphUI.Monitors.tpsDataDay = datatransDay;
                    tpsSecCount = 0;
                }

                if (tpsMinCount >= 60 || monitor.tpsFirstData) {
                    datatransDay = sliceFirstData(datatransDay, dataView.Days);
                    if (monitor.tpsFirstData || delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)) {
                        transDetailsDay = MonitorGraphUI.saveLocalStorage(transDetailsArrDay, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": 0 }, MonitorGraphUI.timeUnit.Day  )
                        datatransDay.push({ "x": new Date(transacDetail["TimeStamp"]), "y": 0 });
                    }
                    MonitorGraphUI.Monitors.tpsDataDay = datatransDay;
                    tpsMinCount = 0;
                }

                if (monitor.tpsFirstData){
                    if(localStorage.transDetails == undefined){
                        datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": null });
                        MonitorGraphUI.Monitors.tpsData = datatrans;
                    }
                    else{
                        if (delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)) {
                            datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": datatrans[datatrans.length - 1].y });
                            transDetailsArr = MonitorGraphUI.saveLocalStorage(transDetailsArr, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": datatrans[datatrans.length - 1].y }, MonitorGraphUI.timeUnit.sec  )
                            MonitorGraphUI.Monitors.tpsData = datatrans;
                        }
                    }
                }
                else{
                    if(localStorage.transDetails == undefined){
                        datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": 0 });
                        MonitorGraphUI.Monitors.tpsData = datatrans;
                    }
                    else{
                        var calculatedValue = parseFloat(delta * 1000.0 / (currentTimerTick - monitor.lastTimerTick)).toFixed(1) * 1;
                        if (delta != 0 || (currentTimedTransactionCount == 0 && monitor.lastTimedTransactionCount == 0)) {
                            datatrans.push({ "x": new Date(transacDetail["TimeStamp"]), "y": calculatedValue });
                            transDetailsArr = MonitorGraphUI.saveLocalStorage(transDetailsArr, {"timestamp": new Date(transacDetail["TimeStamp"]), "transaction": calculatedValue }, MonitorGraphUI.timeUnit.sec  )
                            MonitorGraphUI.Monitors.tpsData = datatrans;
                        }
                    }
                }
                monitor.tpsFirstData = false;
            }

            localStorage.transDetails = JSON.stringify(transDetailsArr)
            localStorage.transDetailsMin = JSON.stringify(transDetailsArrMin)
            localStorage.transDetailsDay = JSON.stringify(transDetailsArrDay)

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


        this.timeUnit = {
            sec: 5,
            min: 30,
            day: 300
        }

        this.RefreshCpu = function (cpuDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var cpuDetailsArr = []
            var cpuDetailsArrMin = []
            var cpuDetailsArrDay = []

            if(localStorage.cpuDetailsMin != undefined)
                cpuDetailsArrMin = JSON.parse(localStorage.cpuDetailsMin)

            if(localStorage.cpuDetailsDay != undefined)
                cpuDetailsArrDay = JSON.parse(localStorage.cpuDetailsDay)

            var cpuData = monitor.cpuData;
            var cpuDataMin = monitor.cpuDataMin;
            var cpuDataDay = monitor.cpuDataHrs;
            var cpuDetail = cpuDetails;


            if ($.isEmptyObject(cpuDetail) || cpuDetail == undefined || !cpuDetail.hasOwnProperty(currentServer) || cpuDetail[currentServer].PERCENT_USED == undefined || cpuDetail[currentServer].TIMESTAMP == undefined)
                return;

            if(localStorage.cpuDetails != undefined)
                cpuDetailsArr = JSON.parse(localStorage.cpuDetails)

            if(monitor.cpuFirstData){
                for(var i = 0; i< cpuDetailsArr.length; i++){
                    sliceFirstData(monitor.cpuData, dataView.Seconds);
                    monitor.cpuData.push({"x": new Date(cpuDetailsArr[i].timestamp),
                        "y": cpuDetailsArr[i].percentUsed
                    })
                }
                for(var j = 0; j< cpuDetailsArrMin.length; j++){
                    sliceFirstData(monitor.cpuDataMin, dataView.Minutes);
                    monitor.cpuDataMin.push({"x": new Date(cpuDetailsArrMin[j].timestamp),
                        "y": cpuDetailsArrMin[j].percentUsed
                    })
                }

                for(var k = 0; k< cpuDetailsArrDay.length; k++){
                    sliceFirstData(monitor.cpuDataHrs, dataView.Days );
                    monitor.cpuDataHrs.push({"x": new Date(cpuDetailsArrDay[k].timestamp),
                        "y": cpuDetailsArrDay[k].percentUsed
                    })
                }

            }

            var percentageUsage = parseFloat(cpuDetail[currentServer].PERCENT_USED).toFixed(1) * 1;
            var timeStamp = cpuDetail[currentServer].TIMESTAMP;


            if (timeStamp >= monitor.cpuMaxTimeStamp) {
                if (percentageUsage < 0)
                    percentageUsage = 0;
                else if (percentageUsage > 100)
                    percentageUsage = 100;

                if (cpuSecCount >= 6 || monitor.cpuFirstData) {
                    cpuDataMin = sliceFirstData(cpuDataMin, dataView.Minutes);
                    if (timeStamp == monitor.cpuMaxTimeStamp) {
                        cpuDataMin.push({ "x": new Date(timeStamp), "y": cpuDataMin[cpuDataMin.length - 1].y });
                        cpuDetailsArrMin = MonitorGraphUI.saveLocalStorage(cpuDetailsArrMin, {"timestamp": new Date(timeStamp), "percentUsed": cpuData[cpuData.length - 1].y}, MonitorGraphUI.timeUnit.min  )
                    } else {
                        cpuDataMin.push({ "x": new Date(timeStamp), "y": percentageUsage });
                        cpuDetailsArrMin = MonitorGraphUI.saveLocalStorage(cpuDetailsArrMin, {"timestamp": new Date(timeStamp), "percentUsed": percentageUsage}, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.cpuDataMin = cpuDataMin;
                    cpuSecCount = 0;
                }
                if (cpuMinCount >= 60 || monitor.cpuFirstData) {
                    cpuDataDay = sliceFirstData(cpuDataDay, dataView.Days);
                    if (timeStamp == monitor.cpuMaxTimeStamp) {
                        cpuDataDay.push({ "x": new Date(timeStamp), "y": cpuDataDay[cpuDataDay.length - 1].y });
                        cpuDetailsArrDay = MonitorGraphUI.saveLocalStorage(cpuDetailsArrDay, {"timestamp": new Date(timeStamp), "percentUsed": cpuData[cpuData.length - 1].y}, MonitorGraphUI.timeUnit.day  )
                    } else {
                        cpuDataDay.push({ "x": new Date(timeStamp), "y": percentageUsage });
                        cpuDetailsArrDay = MonitorGraphUI.saveLocalStorage(cpuDetailsArrDay, {"timestamp": new Date(timeStamp), "percentUsed": percentageUsage}, MonitorGraphUI.timeUnit.day )
                    }
                    MonitorGraphUI.Monitors.cpuDataHrs = cpuDataDay;
                    cpuMinCount = 0;
                }
                cpuData = sliceFirstData(cpuData, dataView.Seconds);
                if (timeStamp == monitor.cpuMaxTimeStamp) {
                    cpuData.push({ "x": new Date(timeStamp), "y": cpuData[cpuData.length - 1].y });
                    cpuDetailsArr = MonitorGraphUI.saveLocalStorage(cpuDetailsArr, {"timestamp": new Date(timeStamp), "percentUsed": cpuData[cpuData.length - 1].y}, MonitorGraphUI.timeUnit.sec  )
                } else {
                    cpuData.push({ "x": new Date(timeStamp), "y": percentageUsage });
                    cpuDetailsArr = MonitorGraphUI.saveLocalStorage(cpuDetailsArr, {"timestamp": new Date(timeStamp), "percentUsed": percentageUsage}, MonitorGraphUI.timeUnit.sec  )
                }
                try{
                    $(".errorMsgLocalStorageFull").hide();
                    localStorage.cpuDetails = JSON.stringify(cpuDetailsArr)
                    localStorage.cpuDetailsMin = JSON.stringify(cpuDetailsArrMin)
                    localStorage.cpuDetailsDay = JSON.stringify(cpuDetailsArrDay)
                }
                catch(e){
                    $(".errorMsgLocalStorageFull").show();
                }
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
            }
            if (timeStamp > monitor.cpuMaxTimeStamp)
                monitor.cpuMaxTimeStamp = timeStamp;
            cpuSecCount++;
            cpuMinCount++;
        };

        this.saveLocalStorage = function(data, newItem, timeUnit){
            var sliderValue = $( "#slider-range-min" ).slider( "value" )
            var slicedData = []
            var interval = (sliderValue * 60) / timeUnit
            if (sliderValue != 0){
                if (data.length >= interval){
                    slicedData = data.slice(1,  (data.length - (data.length - interval)))
                } else {
                    slicedData = data
                }
            slicedData.push(newItem)
            }
            return slicedData;
        }

        this.savePartitionDataToLocalStorage = function(data, newItem, timeUnit, keyIndex){
            var sliderValue = $( "#slider-range-min" ).slider( "value" )
            var values = data[keyIndex].values
            var slicedData = []
            var interval = (sliderValue * 60) / timeUnit
            if (values.length >= interval){
                slicedData = values.slice(1,  (values.length - (values.length - interval)))
            } else {
                slicedData = values
            }
            slicedData.push(newItem)
            data[keyIndex].values = slicedData
            return data;
        }

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

            if (dataMapperSec == undefined || $.isEmptyObject(dataMapperSec))
                return

            if (dataMapperDay == undefined || $.isEmptyObject(dataMapperDay))
                return

            if (dataMapperMin == undefined || $.isEmptyObject(dataMapperMin))
                return

            var partitionData = monitor.partitionData;
            var partitionDataMin = monitor.partitionDataMin;
            var partitionDataDay = monitor.partitionDataDay;
            var partitionDetail = partitionDetails;
            var partitionDetailsArr = [];
            var partitionDetailsArrMin = [];
            var partitionDetailsArrDay = [];


            if(localStorage.partitionDetailsMin != undefined)
                partitionDetailsArrMin = JSON.parse(localStorage.partitionDetailsMin)
            else
                partitionDetailsArrMin = JSON.stringify(monitor.partitionDataMin);

            if(localStorage.partitionDetailsDay != undefined)
                partitionDetailsArrDay = JSON.parse(localStorage.partitionDetailsDay)
            else
                partitionDetailsArrDay = JSON.stringify(monitor.partitionDataDay);

            if(localStorage.partitionDetails != undefined)
                partitionDetailsArr = JSON.parse(localStorage.partitionDetails)
            else
                partitionDetailsArr = JSON.stringify(monitor.partitionData);

            if(monitor.partitionFirstData){
                if(typeof(partitionDetailsArr) != "object" )
                    partitionDetailsArr = JSON.parse(partitionDetailsArr)
                for(var i = 0; i< partitionDetailsArr.length; i++){
                    keyIndexSec =  i;
                    if(partitionDetailsArr.length != 0)
                        monitor.partitionData[keyIndexSec]["values"] = []
                    for(var b = 0; b < partitionDetailsArr[i]["values"].length; b++){
                        monitor.partitionData[keyIndexSec]["values"].push({"x": new Date(partitionDetailsArr[i]["values"][b].x), "y": partitionDetailsArr[i]["values"][b].y})
                    }
                }

                if(typeof(partitionDetailsArrMin) != "object" )
                    partitionDetailsArrMin = JSON.parse(partitionDetailsArrMin)
                for(var j = 0; j< partitionDetailsArrMin.length; j++){
                      keyIndexMin =  j;
                    if(partitionDetailsArrMin.length != 0)
                        monitor.partitionDataMin[keyIndexMin]["values"] = []
                    for(var a = 0; a < partitionDetailsArrMin[j]["values"].length; a++){
                        monitor.partitionDataMin[keyIndexMin]["values"].push({"x": new Date(partitionDetailsArrMin[j]["values"][a].x), "y": partitionDetailsArrMin[j]["values"][a].y})
                    }
                }
                if(typeof(partitionDetailsArrDay) != "object" )
                    partitionDetailsArrDay = JSON.parse(partitionDetailsArrDay)
                for(var k = 0; k< partitionDetailsArrDay.length; k++){
//                        keyIndexDay =  partitionDetailsArrDay[k].key.substr(partitionDetailsArrDay[k].key.length - 6, 1)
                   keyIndexDay = k;
                   if(partitionDetailsArrDay.length != 0)
                        monitor.partitionDataDay[keyIndexMin]["values"] = []
                   for(var c = 0; c < partitionDetailsArrDay[k]["values"].length; c++){
                       monitor.partitionDataDay[keyIndexDay]["values"].push({"x": new Date(partitionDetailsArrDay[k]["values"][c].x), "y": partitionDetailsArrDay[k]["values"][c].y})
                   }
                }
            }

            if ($.isEmptyObject(partitionDetail) || partitionDetail == undefined ||partitionDetail["partitionDetail"]["timeStamp"] == undefined)
                return;

            var timeStamp = partitionDetails["partitionDetail"]["timeStamp"];
            if (timeStamp >= monitor.partitionMaxTimeStamp) {
                $.each(partitionDetail["partitionDetail"], function(datatype, datavalue) {
                    $.each(datavalue, function(partitionKey, partitionValue) {
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
                                if (timeStamp == monitor.partitionMaxTimeStamp) {
                                    partitionDataMin[keyIndex]["values"].push({"x": new Date(timeStamp), "y": partitionDataMin[keyIndex]["values"][partitionDataMin[keyIndex]["values"].length - 1].y });
                                    partitionDetailsArrMin = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArrMin, {"x": new Date(timeStamp), "y": partitionDataMin[keyIndex]["values"][partitionDataMin[keyIndex]["values"].length - 1].y }, MonitorGraphUI.timeUnit.min, keyIndex)
                                } else {
                                    partitionDataMin[keyIndex]["values"].push({ 'x': new Date(timeStamp), 'y': percentValue });
                                    partitionDetailsArrMin = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArrMin, { 'x': new Date(timeStamp), 'y': percentValue }, MonitorGraphUI.timeUnit.min, keyIndex)
                                }
                                MonitorGraphUI.Monitors.partitionDataMin = partitionDataMin;
                            }
                        }

                        if (partitionMinCount >= 60 || monitor.partitionFirstData) {
                            var keyIndexDay = dataMapperDay[keyValue];
                            partitionDataDay[keyIndexDay]["values"] = sliceFirstData(partitionDataDay[keyIndexDay]["values"], dataView.Days);
                            if (timeStamp == monitor.partitionMaxTimeStamp) {
                                partitionDataDay[keyIndexDay]["values"].push({ "x": new Date(timeStamp), "y": partitionDataDay[keyIndexDay]["values"][partitionDataDay[keyIndexDay]["values"].length - 1].y });
                                partitionDetailsArrDay = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArrDay, { "x": new Date(timeStamp), "y": partitionDataDay[keyIndexDay]["values"][partitionDataDay[keyIndexDay]["values"].length - 1].y }, MonitorGraphUI.timeUnit.day, keyIndexDay)
                            } else {
                                partitionDataDay[keyIndexDay]["values"].push({ 'x': new Date(timeStamp), 'y': percentValue });
                                partitionDetailsArrDay = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArrDay, { 'x': new Date(timeStamp), 'y': percentValue }, MonitorGraphUI.timeUnit.day, keyIndexDay)
                            }
                            MonitorGraphUI.Monitors.partitionDataDay = partitionDataDay;
                        }

                        var keyIndexSec = dataMapperSec[keyValue];

                        partitionData[keyIndexSec]["values"] = sliceFirstData(partitionData[keyIndexSec]["values"], dataView.Seconds);
                        if (timeStamp == monitor.partitionMaxTimeStamp) {
                            partitionData[keyIndexSec]["values"].push({"x": new Date(timeStamp), "y": partitionData[keyIndexSec]["values"][partitionData[keyIndexSec]["values"].length - 1].y });
                            partitionDetailsArr = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArr, {"x": new Date(timeStamp), "y": partitionData[keyIndexSec]["values"][partitionData[keyIndexSec]["values"].length - 1].y }, MonitorGraphUI.timeUnit.sec, keyIndexSec)
                        } else {
                            partitionData[keyIndexSec].values.push({ 'x': new Date(timeStamp), 'y': percentValue });
                            partitionDetailsArr = MonitorGraphUI.savePartitionDataToLocalStorage(partitionDetailsArr, { 'x': new Date(timeStamp), 'y': percentValue }, MonitorGraphUI.timeUnit.sec, keyIndexSec  )

                        }
                        MonitorGraphUI.Monitors.partitionData = partitionData;
                    });
                });

                localStorage.partitionDetails = JSON.stringify(partitionDetailsArr)
                localStorage.partitionDetailsMin = JSON.stringify(partitionDetailsArrMin)
                localStorage.partitionDetailsDay = JSON.stringify(partitionDetailsArrDay)
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
            }
            if (timeStamp > monitor.partitionMaxTimeStamp)
                monitor.partitionMaxTimeStamp = timeStamp;

            partitionSecCount++;
            partitionMinCount++;
        };

        this.RefreshDrReplicationGraph = function (drDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var drData = monitor.drReplicationData;
            var drDataMin = monitor.drReplicationDataMin;
            var drDataDay = monitor.drReplicationDataDay;
            var drDetail = drDetails;
            var drDetailsArr = []
            var drDetailsArrMin = []
            var drDetailsArrDay = []

            if(localStorage.drDetailsMin != undefined)
                drDetailsArrMin = JSON.parse(localStorage.drDetailsMin)

            if(localStorage.drDetailsDay != undefined)
                drDetailsArrDay = JSON.parse(localStorage.drDetailsDay)


            if ($.isEmptyObject(drDetail) || drDetail == undefined || drDetail["DR_GRAPH"].REPLICATION_RATE_1M == undefined || drDetail["DR_GRAPH"].TIMESTAMP == undefined)
                return;

            if(localStorage.drDetails != undefined)
                drDetailsArr = JSON.parse(localStorage.drDetails)

            if(monitor.drFirstData){
                for(var i = 0; i< drDetailsArr.length; i++){
                    sliceFirstData(monitor.drReplicationData, dataView.Seconds);
                    monitor.drReplicationData.push({"x": new Date(drDetailsArr[i].timestamp),
                        "y": drDetailsArr[i].replicationRate
                    })
                }
                for(var j = 0; j< drDetailsArrMin.length; j++){
                    sliceFirstData(monitor.drReplicationDataMin, dataView.Minutes);
                    monitor.drReplicationDataMin.push({"x": new Date(drDetailsArrMin[j].timestamp),
                        "y": drDetailsArrMin[j].replicationRate
                    })
                }

                for(var k = 0; k< drDetailsArrDay.length; k++){
                    sliceFirstData(monitor.drReplicationDataDay, dataView.Days );
                    monitor.drReplicationDataDay.push({"x": new Date(drDetailsArrDay[k].timestamp),
                        "y": drDetailsArrDay[k].replicationRate
                    })
                }

            }

            var timeStamp = drDetail["DR_GRAPH"].TIMESTAMP;
            if (timeStamp >= monitor.drMaxTimeStamp) {
                var plottingPoint = parseFloat(drDetail["DR_GRAPH"].REPLICATION_RATE_1M).toFixed(1) * 1;

                if (drSecCount >= 6 || monitor.drFirstData) {
                    drDataMin = sliceFirstData(drDataMin, dataView.Minutes);
                    if (timeStamp == monitor.drMaxTimeStamp) {
                        drDataMin.push({ "x": new Date(timeStamp), "y": drDataMin[drDataMin.length - 1].y });
                        drDetailsArrMin = MonitorGraphUI.saveLocalStorage(drDetailsArrMin, {"timestamp": new Date(timeStamp), "replicationRate": drDataMin[drDataMin.length - 1].y}, MonitorGraphUI.timeUnit.min  )
                    } else {
                        drDataMin.push({ "x": new Date(timeStamp), "y": plottingPoint });
                        drDetailsArrMin = MonitorGraphUI.saveLocalStorage(drDetailsArrMin, {"timestamp": new Date(timeStamp), "replicationRate": plottingPoint}, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.drReplicationDataMin = drDataMin;
                    drSecCount = 0;
                }
                if (drMinCount >= 60 || monitor.drFirstData) {
                    drDataDay = sliceFirstData(drDataDay, dataView.Days);
                    if (timeStamp == monitor.drMaxTimeStamp) {
                        drDataDay.push({ "x": new Date(timeStamp), "y": drDataDay[drDataDay.length - 1].y });
                        drDetailsArrDay = MonitorGraphUI.saveLocalStorage(drDetailsArrDay, {"timestamp": new Date(timeStamp), "replicationRate": drDataDay[drDataDay.length - 1].y}, MonitorGraphUI.timeUnit.day  )
                    } else {
                        drDataDay.push({ "x": new Date(timeStamp), "y": plottingPoint });
                        drDetailsArrDay = MonitorGraphUI.saveLocalStorage(drDetailsArrDay, {"timestamp": new Date(timeStamp), "replicationRate": plottingPoint}, MonitorGraphUI.timeUnit.day  )
                    }
                    MonitorGraphUI.Monitors.drReplicationDataDay = drDataDay;
                    drMinCount = 0;
                }
                drData = sliceFirstData(drData, dataView.Seconds);
                if (timeStamp == monitor.drMaxTimeStamp) {
                    drData.push({ "x": new Date(timeStamp), "y": drData[drData.length - 1].y });
                    drDetailsArr = MonitorGraphUI.saveLocalStorage(drDetailsArr, {"timestamp": new Date(timeStamp), "replicationRate": drData[drData.length - 1].y}, MonitorGraphUI.timeUnit.sec  )
                } else {
                    drData.push({ "x": new Date(timeStamp), "y": plottingPoint });
                    drDetailsArr = MonitorGraphUI.saveLocalStorage(drDetailsArr, {"timestamp": new Date(timeStamp), "replicationRate": drData[drData.length - 1].y}, plottingPoint  )
                }

                localStorage.drDetails = JSON.stringify(drDetailsArr)
                localStorage.drDetailsMin = JSON.stringify(drDetailsArrMin)
                localStorage.drDetailsDay = JSON.stringify(drDetailsArrDay)

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
            }
            if (timeStamp > monitor.drMaxTimeStamp)
                monitor.drMaxTimeStamp = timeStamp;
            drSecCount++;
            drMinCount++;
        };

        this.RefreshCommandLog = function (cmdLogDetails, currentServer, graphView, currentTab) {
            var monitor = MonitorGraphUI.Monitors;
            var cmdLogData = monitor.cmdLogData;
            var cmdLogDataMin = monitor.cmdLogDataMin;
            var cmdLogDataDay = monitor.cmdLogDataDay;
            var cmdLogDetail = cmdLogDetails;
            var cmdLogArr = []
            var cmdLogArrMin = []
            var cmdLogArrDay = []
            var overlayDataArr = []

            if(localStorage.cmdLogMin != undefined)
                cmdLogArrMin = JSON.parse(localStorage.cmdLogMin)
            else{
                cmdLogArrMin =  JSON.stringify(MonitorGraphUI.convertDataFormat(cmdLogDataMin))
                cmdLogArrMin = JSON.parse(cmdLogArrMin)
            }
            if(localStorage.cmdLogDay != undefined)
                cmdLogArrDay = JSON.parse(localStorage.cmdLogDay)
            else {
                cmdLogArrDay = JSON.stringify(MonitorGraphUI.convertDataFormat(cmdLogDataDay))
                cmdLogArrDay = JSON.parse(cmdLogArrDay)
            }

            if(localStorage.cmdLog != undefined)
                cmdLogArr = JSON.parse(localStorage.cmdLog)
            else{
                cmdLogArr =  JSON.stringify(MonitorGraphUI.convertDataFormat(cmdLogData))
                cmdLogArr =  JSON.parse(cmdLogArr)
            }


            if(localStorage.SnapshotOverlayData != undefined)
                overlayDataArr =  JSON.parse(localStorage.SnapshotOverlayData)

            if(monitor.cmdLogFirstData){
                if(cmdLogArr.length != 0)
                    cmdLogData = []
                for(var i = 0; i< cmdLogArr.length; i++){
                    cmdLogData.push({"x": new Date(cmdLogArr[i].timestamp),
                        "y": cmdLogArr[i].outstandingTxn
                    })
                }
                if(cmdLogArrMin.length != 0)
                    cmdLogDataMin = []
                for(var j = 0; j< cmdLogArrMin.length; j++){
                    cmdLogDataMin.push({"x": new Date(cmdLogArrMin[j].timestamp),
                        "y": cmdLogArrMin[j].outstandingTxn
                    })
                }
                if(cmdLogArrDay.length != 0)
                    cmdLogDataDay = []
                for(var k = 0; k< cmdLogArrDay.length; k++){
                    cmdLogDataDay.push({"x": new Date(cmdLogArrDay[k].timestamp),
                        "y": cmdLogArrDay[k].outstandingTxn
                    })
                }

                if(overlayDataArr.length != 0){
                    cmdLogOverlay = MonitorGraphUI.SaveSnapshotOverlay(overlayDataArr)
                }
            }

            if ($.isEmptyObject(cmdLogDetail) || cmdLogDetail == undefined || cmdLogDetail[currentServer].OUTSTANDING_TXNS == undefined || cmdLogDetail[currentServer].TIMESTAMP == undefined)
                return;

            var timeStamp = cmdLogDetail[currentServer].TIMESTAMP;
            if (timeStamp >= monitor.cmdLogMaxTimeStamp) {
                var outStandingTxn = parseFloat(cmdLogDetail[currentServer].OUTSTANDING_TXNS).toFixed(1) * 1;

                if (cmdLogSecCount >= 6 || monitor.cmdLogFirstData) {
                    cmdLogDataMin = sliceFirstData(cmdLogDataMin, dataView.Minutes);
                    if (timeStamp == monitor.cmdLogMaxTimeStamp) {
                        cmdLogDataMin.push({ "x": new Date(timeStamp), "y": cmdLogDataMin[cmdLogDataMin.length - 1].y });
                        cmdLogArrMin = MonitorGraphUI.saveLocalStorage(cmdLogArrMin, {"timestamp": new Date(timeStamp), "outstandingTxn": cmdLogDataMin[cmdLogDataMin.length - 1].y}, MonitorGraphUI.timeUnit.min  )
                    } else {
                        cmdLogDataMin.push({ "x": new Date(timeStamp), "y": outStandingTxn });
                        cmdLogArrMin = MonitorGraphUI.saveLocalStorage(cmdLogArrMin, {"timestamp": new Date(timeStamp), "outstandingTxn": outStandingTxn}, MonitorGraphUI.timeUnit.min  )
                    }
                    MonitorGraphUI.Monitors.cmdLogDataMin = cmdLogDataMin;
                    cmdLogSecCount = 0;
                }
                if (cmdLogMinCount >= 60 || monitor.cmdLogFirstData) {
                    cmdLogDataDay = sliceFirstData(cmdLogDataDay, dataView.Days);
                    if (timeStamp == monitor.cmdLogMaxTimeStamp) {
                        cmdLogDataDay.push({ "x": new Date(timeStamp), "y": cmdLogDataDay[cmdLogDataDay.length - 1].y });
                        cmdLogArrDay = MonitorGraphUI.saveLocalStorage(cmdLogArrDay, {"timestamp": new Date(timeStamp), "outstandingTxn": cmdLogDataDay[cmdLogDataDay.length - 1].y}, MonitorGraphUI.timeUnit.day  )
                    } else {
                        cmdLogDataDay.push({ "x": new Date(timeStamp), "y": outStandingTxn });
                        cmdLogArrDay = MonitorGraphUI.saveLocalStorage(cmdLogArrDay, {"timestamp": new Date(timeStamp), "outstandingTxn": outStandingTxn}, MonitorGraphUI.timeUnit.day  )
                    }
                    MonitorGraphUI.Monitors.cmdLogDataDay = cmdLogDataDay;
                    cmdLogMinCount = 0;
                }
                cmdLogData = sliceFirstData(cmdLogData, dataView.Seconds);
                if (timeStamp == monitor.cmdLogMaxTimeStamp) {
                    cmdLogData.push({ "x": new Date(timeStamp), "y": cmdLogData[cmdLogData.length - 1].y });
                    cmdLogArr = MonitorGraphUI.saveLocalStorage(cmdLogArr, {"timestamp": new Date(timeStamp), "outstandingTxn": cmdLogData[cmdLogData.length - 1].y}, MonitorGraphUI.timeUnit.sec  )

                } else {
                    cmdLogData.push({ "x": new Date(timeStamp), "y": outStandingTxn });
                    cmdLogArr = MonitorGraphUI.saveLocalStorage(cmdLogArr, {"timestamp": new Date(timeStamp), "outstandingTxn": outStandingTxn}, MonitorGraphUI.timeUnit.sec  )
                }
                MonitorGraphUI.Monitors.cmdLogData = cmdLogData;

                localStorage.cmdLog = JSON.stringify(cmdLogArr)
                localStorage.cmdLogMin = JSON.stringify(cmdLogArrMin)
                localStorage.cmdLogDay = JSON.stringify(cmdLogArrDay)

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
                        $.each(cmdLogOverlay, function(partitionKey, partitionValue) {
                            var x1 = partitionValue.x;
                            if (x1 == cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME)
                                isDuplicate = true;
                            else
                                isDuplicate = false;
                        });
                        if (!isDuplicate)
                            cmdLogOverlay.push({ "startTime": cmdLogDetail[currentServer].SNAPSHOTS[i].START_TIME, "endTime": cmdLogDetail[currentServer].SNAPSHOTS[i].END_TIME });
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

                localStorage.SnapshotOverlayData = JSON.stringify(MonitorGraphUI.SaveSnapshotOverlay(cmdLogOverlay))

                $.each(cmdLogOverlay, function(partitionKey, partitionValue) {
                    var x1 = MonitorGraphUI.ChartCommandlog.xScale()(partitionValue.startTime);
                    var x2 = MonitorGraphUI.ChartCommandlog.xScale()(partitionValue.endTime);
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

            }
            if (timeStamp > monitor.cmdLogMaxTimeStamp) {
                monitor.cmdLogMaxTimeStamp = timeStamp;
            }
            cmdLogSecCount++;
            cmdLogMinCount++;
        };

        this.convertDataFormat = function(cmdLogData){
            var requiredFormat = []
            for(var i = 0; i < cmdLogData.length; i++){
                requiredFormat.push({"timestamp": cmdLogData[i].x, "outstandingTxn": cmdLogData[i].y})
            }
            return requiredFormat;
        }

        this.convertDataFormatForMemory = function(memoryData){
            var requiredFormat = []
            for(var i = 0; i < memoryData.length; i++){
                requiredFormat.push({"timestamp": memoryData[i].x, "physicalMemory": memoryData[i].y})
            }
            return requiredFormat;
        }

        this.SaveSnapshotOverlay = function(snapshotData, timeUnit){
            var interval_end = new Date()
            var interval_start = new Date()
            var interval = $( "#slider-range-min" ).slider( "value" )
            interval_end.setMinutes(interval_end.getMinutes() - interval);
            snapshotDataArr = [];
            for(var i = 0; i < snapshotData.length; i++){
                start_timeStamp =  snapshotData[i].startTime;
                stop_timeStamp = snapshotData[i].endTime;
                if(start_timeStamp >= interval_end.getTime() && start_timeStamp <= interval_start.getTime()
                && start_timeStamp >= interval_end.getTime() && start_timeStamp <= interval_start.getTime()){
                    snapshotDataArr.push(snapshotData[i])
                }
            }
            return snapshotDataArr;
        }

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

