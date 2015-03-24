$(function () {
    // global vars
    var mySqlLoading = false;
    var lambdaLoading = false;

    // Declare a proxy to reference the hub. 
    var updateMySQL = $.connection.updateHub;

    // Create a function that the hub can call to broadcast messages.
    updateMySQL.client.updateMySQL = function () {
        // get the time
        var end = new Date().getTime();
        var begin = end - (60 * 1000);

        // set spread to minute
        afterSetExtremes({ 'min': begin, 'max': end });

        //update the extremes
        $('#MySQL').highcharts().xAxis[0].setExtremes(begin, end, true);
    };

    // Start the connection
    $.connection.hub.start();

    /**
     * Load new data depending on the selected min and max
     */    
    function afterSetExtremes(e) {
        if (e == null) {
            e = {
                'min': 0,
                'max': new Date().getTime()
            };
        }

        var MySQL = $('#MySQL').highcharts();
        var Lambda = $('#Lambda').highcharts();

        //MySQL.showLoading('Loading data from server...');
        //Lambda.showLoading('Loading data from server...');


        if (mySqlLoading != true) {
            // set the loading, to prevent this function running concurrently
            mySqlLoading = true;

            // Get the data
            $.getJSON('api/sensorData/?start=' + Math.round(e.min) + '&end=' + Math.round(e.max), function (sensorData) {
                // for each series returned, either add it to the chart, or replace the existing chart data with
                // the value of the new data
                $.each($.parseJSON(sensorData), function (itemNo, item) {
                    // does this series exist?
                    if (MySQL.get('Sensor ' + itemNo) == null) {
                        // no - add the series
                        MySQL.addSeries({
                            name: 'Sensor ' + itemNo,
                            id: 'Sensor ' + itemNo,
                            data: item
                        }, true);
                    }
                    else {
                        // yes - replace the data
                        MySQL.get('Sensor ' + itemNo).setData(item);
                    }
                });

                MySQL.hideLoading();
                mySqlLoading = false;
            });
        };

        if (lambdaLoading != true) {
            lambdaLoading = true;
            Lambda.hideLoading();
            lambdaLoading = false;
        }
    }

    // MySql
    // create the chart
    $('#MySQL').highcharts('StockChart', {
        chart: {
            //type: 'candlestick',
            zoomType: 'x'
        },

        navigator: {
            adaptToUpdatedData: false,
        },

        scrollbar: {
            liveRedraw: false
        },

        title: {
            text: 'MySQL Database'
        },

        rangeSelector: {
            buttons: [{
                type: 'minute',
                count: 1,
                text: '1m'
            }, {
                type: 'hour',
                count: 1,
                text: '1h'
            }, {
                type: 'day',
                count: 1,
                text: '1d'
            }, {
                type: 'month',
                count: 1,
                text: '1m'
            }, {
                type: 'month',
                count: 3,
                text: '3m'
            }, {
                type: 'year',
                count: 1,
                text: '1y'
            }, {
                type: 'all',
                text: 'All'
            }],
            inputEnabled: false, // it supports only days
            selected: 7, // all
            allButtonsEnabled: true
        },

        xAxis: {
            events: {
                afterSetExtremes: afterSetExtremes
            },
            minRange: 3600 * 1000 // one hour
        },
    });  


    // create the chart
    $('#Lambda').highcharts('StockChart', {
        chart: {
            type: 'candlestick',
            zoomType: 'x'
        },

        navigator: {
            adaptToUpdatedData: false,
        },

        scrollbar: {
            liveRedraw: false
        },

        title: {
            text: 'Lambda Architecture'
        },

        rangeSelector: {
            enabled: false
        },

        xAxis: {
            events: {
                afterSetExtremes: afterSetExtremes
            },
            minRange: 3600 * 1000 // one hour
        },

        yAxis: {
            floor: 0
        }
    });

    afterSetExtremes(null);
});