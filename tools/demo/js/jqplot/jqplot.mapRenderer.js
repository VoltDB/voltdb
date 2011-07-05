/**
 * Copyright (c) 2010 John Cheng
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
(function($) {
    
    // Class: $.jqplot.MapRenderer
    // A plugin renderer for jqPlot to draw a heat map.
    $.jqplot.MapRenderer = function(){
        $.jqplot.LineRenderer.call(this);
    };
    
    $.jqplot.MapRenderer.prototype = new $.jqplot.LineRenderer();
    $.jqplot.MapRenderer.prototype.constructor = $.jqplot.MapRenderer;
    
    // called with scope of series.
    $.jqplot.MapRenderer.prototype.init = function(options) {
        // Group: Properties
        //
        // prop: mapVarName
        // Name of an JavaScript variable that contains data points for a map. 
        // NOTE: We eval the name to map data instead of directly referencing map data, otherwise jqPlot 
        // will need to make multiple deep-copies of the rendererOptions of the map data, which is slow on IE.
        this.mapVarName = null;
        $.extend(true, this, options);
        // set the shape renderer options
        var opts = {lineJoin:'miter', lineCap:'round', fill:true, isarc:false, strokeStyle:this.color, fillStyle:this.color, closePath:this.fill};
        this.renderer.shapeRenderer.init(opts);
        // set the shadow renderer options
        var sopts = {lineJoin:'miter', lineCap:'round', fill:true, isarc:false, angle:this.shadowAngle, offset:this.shadowOffset, alpha:this.shadowAlpha, depth:this.shadowDepth, closePath:this.fill};
        this.renderer.shadowRenderer.init(sopts);
        this.name = 'mapRenderer';
    };

    // mapData: An object in the form of 
    //   ( 
    //      size: [width, height],
    //      nodes: ({"node1": [ draw instructions... ], "node2": [ draw instructions...]})
    //   )
    // nodeParams: A map where key is a node id in the mapData, and corresponding value holds the parameters for rendering that node
    //   {
    //      "node1": {
    //              fillStyle: '#fff'
    //              },
    //      "node2": {
    //              fillStyle: '#ddd'
    //              }
    //   }
    // defaultNodeParams: Default parameters that can be applied to all nodes
    //  
    function _drawMap(ctx, mapData, nodeParams, defaultNodeParams) {
        defaultNodeParams = $.extend({}, {
            fillStyle:'#fff'
        }, defaultNodeParams);
        for ( var node in mapData.nodes ) {
            var currParams = $.extend({}, defaultNodeParams, nodeParams[node]);
            if ( nodeParams[node] ) {
                ctx.fillStyle = currParams.fillStyle;
                $.each(mapData.nodes[node], function(i, cmd) {
                    if ( cmd[0] == 'z' ) {
                        ctx.closePath();
                        ctx.stroke();
                        ctx.fill();
                    } else {
                        var point = null;
                        // scale the drawing instructions according to specified size
                        if ( defaultNodeParams.scale != undefined ) {
                            point = new Array(cmd[1].length);
                            for ( var i = 0; i < cmd[1].length; i++ ) {
                                point[i] = cmd[1][i] * defaultNodeParams.scale;
                            }
                        } else {
                            point = cmd[1];
                        }
                        // execute drawing instrutions
                        switch(cmd[0]) {
                            case 'M': ctx.beginPath();ctx.moveTo.apply(ctx, point); break;
                            case 'L': ctx.lineTo.apply(ctx, point); break;
                            case 'C': ctx.bezierCurveTo.apply(ctx, point); break;
                            default: break;
                        }
                    }
                });
            }
        }
    }
    
    $.jqplot.MapRenderer.prototype.draw = function(ctx, gridData, options) {
        var mapData = eval(this.mapVarName);
        if ( mapData && this.data ) {
            var scale = 1;
            // for now, we do not try to scale map data without a size specification
            if ( mapData.size ) {
                var scaleX = Math.max(1, this._plotDimensions.width-40)/mapData.size[0];
                var scaleY = Math.max(1, this._plotDimensions.height-40)/mapData.size[1];
                scale = Math.min(scaleX, scaleY);
            }

            // calculate the color intensity of each node, base on its relative
            // value compared to min & max. The minimum value in the series gets
            // 10% of the "base color", while the maximum value gets 100% of base
            // color
            var nodeParams = {};
            var min = 0, max = 0;
            $(this.data).each(function(i, item) {
                min = Math.min(min, item[1]);
                max = Math.max(max, item[1]);
            });
            // rgb = base color
            var rgb = $.jqplot.getColorComponents(this.seriesColors[this.index]);
            var c1 = rgb[0]+((255-rgb[0])*0.9)
            var c2 = rgb[1]+((255-rgb[1])*0.9)
            var c3 = rgb[2]+((255-rgb[2])*0.9)
            if ( max == min ) { max = min+1 } // prevents divide by zero
            var m1 = (c1-rgb[0])/(min-max);
            var m2 = (c2-rgb[1])/(min-max);
            var m3 = (c3-rgb[2])/(min-max);
            $(this.data).each(function(i, item) {
                var rgbAdj = [Math.round((item[1]*m1)+c1), Math.round((item[1]*m2)+c2), Math.round((item[1]*m3)+c3)];
                nodeParams[item[0]] = { fillStyle: $.jqplot.rgb2hex('rgba('+rgbAdj.join(',')+')') }
            });
            _drawMap(ctx, mapData, nodeParams, {scale: scale});

            // If this is the last series, then identify all the nodes that have
            // not been drawn and draw them, otherwise there will be pieces of
            // the map missing
            if ( this.index+1 == this._xaxis._series.length ) {
                var notDrawn = {};
                for ( node in mapData.nodes ) {
                    notDrawn[node] = true;
                }
                $(this._xaxis._series).each(function(i, item) {
                    $(item.data).each(function(i, node) {
                            notDrawn[node[0]] = false;
                    });
                });
                var nodeParams = {};
                for ( node in notDrawn ) {
                    if ( notDrawn[node] ) {
                        nodeParams[node] = { };
                    }
                }
                _drawMap(ctx, mapData, nodeParams, {scale: scale});
            }
        }
    };

    $.jqplot.MapRenderer.prototype.drawShadow = function(ctx, gridData, options) {
        // JCHENG: Drawing shadow not currently supported
    };
})(jQuery);
